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

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class WatermarkOutOfOrdernessDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // env.setParallelism(1);
        env.setParallelism(2);
        /**
         * 演示watermark多并行度下的传递
         * 1 接收到多个，取最小
         * 2 往下游多个发送广播
         */

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
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    return element.getTs() * 1000L;
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

/**
 * 1.都是周期性生成的：默认200ms
 * 2.有序流：watermark = 当前最大的事件时间 - 1ms
 * 3.乱序流：watermark = 当前最大的事件时间 - 延迟时间 - 1ms
 */