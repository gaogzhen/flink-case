package com.gaogzhen.datastream.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class IntervalJoinDemo {
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

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r -> r.f0);

        ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .process(
                        new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            /**
                             * 两条流的数据匹配上，才会调用该方法
                             * @param left The left element of the joined pair.
                             * @param right The right element of the joined pair.
                             * @param ctx A context that allows querying the timestamps of the left, right and joined pair.
                             *     In addition, this context allows to emit elements on a side output.
                             * @param out The collector to emit resulting elements to.
                             * @throws Exception
                             */
                            @Override
                            public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                                // 输出关联上的数据
                                out.collect(left + "===" + right);
                            }
                        }
                )
                .print();

        env.execute();
    }
}
