package com.gaogzhen.datastream.watermark;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class WatermarkMonoDemo {
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
                // 1.1 指定watermark生成：升序，没有等待时间
                .<WaterSensor>forMonotonousTimestamps()
                // 1.2 指定时间戳分配器，从数据中提取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        // 返回的时间戳，单位毫秒
                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensorDs.assignTimestampsAndWatermarks(watermarkStrategy);

        sensorWithWatermark.keyBy(WaterSensor::getId)
                // 使用事件时间语义
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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
                ).print();


        env.execute();
    }
}
