package com.gaogzhen.datastream.watermark;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class WatermarkAllowLatenessDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        // 指定watermark 策略
        // 1. 定义watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：乱序，有等待时间
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 1.2 指定时间戳分配器，从数据中提取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                    // 返回的时间戳，单位毫秒
                    return element.getTs() * 1000L;
                });

        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensorDs.assignTimestampsAndWatermarks(watermarkStrategy);

        OutputTag<WaterSensor> lateTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorWithWatermark.keyBy(WaterSensor::getId)
                // 使用事件时间语义
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2)) // 推迟2s关窗
                .sideOutputLateData(lateTag) // 关窗之后的迟到数据，放入侧输出流
                .process(
                        new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                                String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                                long count = elements.spliterator().estimateSize();
                                out.collect("调用process，key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据==>" + elements);
                            }
                        }
                );
        process.print();
        process.getSideOutput(lateTag).printToErr();

        env.execute();
    }
}
/**
 * 1 乱序与迟到的区别
 * 乱序：数据的顺序，数据的生成顺序与处理顺序不一致
 * 迟到：当前时间戳 < 当前的watermark
 * 2 乱序、迟到输出的处理
 * 2.1 watermark中设置乱序等待时间
 * 2.2 如果开窗，设置窗口允许迟到
 *  2.2.1推迟关窗时间，在关窗之前，迟到数据还能被窗口计算，来一条迟到数据计算一次
 *  2.2.2 关窗后，迟到数据不会计算
 * 3 关窗后迟到数据，放入侧输出流
 *
 * 如果watermark等等3s，窗口允许迟到2s，为什么不知道设置等待5s或者允许迟到5s？
 * 1 watermark等待时间设置太大，影响计算的延迟
 * 2 窗口允许迟到，是对大部分迟到数据的处理，尽量保证结果的准确
 *  如果设置允许迟到5s，会导致频繁计算输出
 *
 *  设置经验
 *  1 watermark等待时间设置，秒级，在乱序和延迟中取舍
 *  2 设置一定的窗口允许迟到，只考虑大部分的迟到数据，极端少数据不考虑
 *  3 极端少迟到很久的数据，放入侧输出流，等待后续处理
 */