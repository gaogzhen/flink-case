package com.gaogzhen.datastream.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class IntervalJoinWithLateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return Tuple2.of(data[0], Integer.valueOf(data[1]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((v, ts) -> v.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.socketTextStream("localhost", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return Tuple3.of(data[0], Integer.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((v, ts) -> v.f1 * 1000L)
                );

        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r -> r.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r -> r.f0);

        OutputTag<Tuple2<String, Integer>> ks1LateTage = new OutputTag<>("ks1-late", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> ks2LateTage = new OutputTag<>("ks2-late", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        /**
         * interval join
         * 1. 只支持事件时间
         * 2. 指定上界、下界的偏移，符号代表向前，正号代表时间向右
         * 3. process中只能处理join上的数据
         * 4. 两条流关联后的watermark，以两条流中最小的为准
         * 5. 如果当前事件时间 < 当前watermark，就是迟到数据，主流process不处理
         *  5.1. between后可以指定将左流或者右流的数据放入侧输出流
         */

        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(ks1LateTage)
                .sideOutputRightLateData(ks2LateTage)
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
                );

        process.getSideOutput(ks1LateTage).printToErr("ks1的迟到数据：");
        process.getSideOutput(ks2LateTage).printToErr("ks2的迟到数据：");
        process.print("主流输出：");
        

        env.execute();
    }
}
