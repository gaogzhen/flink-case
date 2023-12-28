package com.gaogzhen.datastream.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class WatermarkJoinDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromElements(
                        Tuple2.of("a1", 1),
                        Tuple2.of("a1", 2),
                        Tuple2.of("b1", 3),
                        Tuple2.of("c1", 4)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((v, ts) -> v.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromElements(
                        Tuple3.of("a1", 1, 1),
                        Tuple3.of("a2", 11, 1),
                        Tuple3.of("b1", 2, 5),
                        Tuple3.of("b1", 12, 5),
                        Tuple3.of("c1", 8, 8),
                        Tuple3.of("d1", 20, 1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((v, ts) -> v.f1 * 1000L)
                );


        /**
         * 1. 同一时间窗口内才能匹配
         * 2. 根据keyBy的key进行匹配管理
         * 3. 只能拿到匹配的数据
         */
        DataStream<String> join = ds1.join(ds2)
                .where(r1 -> r1.f0)
                .equalTo(r2 -> r2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((v1, v2) -> v1 + "====" + v2);

        join.print();


        env.execute();
    }
}
