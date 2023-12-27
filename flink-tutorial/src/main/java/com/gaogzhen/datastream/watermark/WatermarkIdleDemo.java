package com.gaogzhen.datastream.watermark;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import com.gaogzhen.datastream.operator.partition.MyPartitioner;
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
public class WatermarkIdleDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(2);

        // 自定义分区器，数据%分区数
        SingleOutputStreamOperator<Integer> socketDs = env.socketTextStream("127.0.0.1", 7777)
                .partitionCustom(new MyPartitioner(), r -> r)
                .map(Integer::parseInt)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r * 1000L)
                                .withIdleness(Duration.ofSeconds(5))
                );



        // 分2组：奇数一组，偶数一组，开10s的滚动窗口
        socketDs.keyBy(r -> r % 2)
                // 使用事件时间语义
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                    new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                        @Override
                        public void process(Integer s, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
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
