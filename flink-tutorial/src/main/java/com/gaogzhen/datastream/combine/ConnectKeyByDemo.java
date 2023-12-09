package com.gaogzhen.datastream.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(4);

        DataStreamSource<Tuple2<Integer, String>> ds1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b1"),
                Tuple2.of(3, "c1")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds2 = env.fromElements(
                Tuple3.of(1, "a3", 1),
                Tuple3.of(1, "a4", 2),
                Tuple3.of(2, "b2", 3),
                Tuple3.of(3, "c2", 4)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = ds1.connect(ds2);

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyBy = connect.keyBy(s1 -> s1.f0, s2 -> s2.f0);

        SingleOutputStreamOperator<String> ret = keyBy.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {

            Map<Integer, List<Tuple2<Integer, String>>> cache1 = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> cache2 = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer f0 = value.f0;
                if (!cache1.containsKey(f0)) {
                    ArrayList arrayList = new ArrayList<>();
                    arrayList.add(value);
                    cache1.put(f0, arrayList);
                } else {
                    cache1.get(f0).add(value);
                }

                if (cache2.containsKey(f0)) {
                    for (Tuple3<Integer, String, Integer> ele : cache2.get(f0)) {
                        out.collect("s1:" + value + "=======s2:" + ele);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                Integer f0 = value.f0;
                if (!cache2.containsKey(f0)) {
                    ArrayList arrayList = new ArrayList<>();
                    arrayList.add(value);
                    cache2.put(f0, arrayList);
                } else {
                    cache2.get(f0).add(value);
                }

                if (cache1.containsKey(f0)) {
                    for (Tuple2<Integer, String> ele : cache1.get(f0)) {
                        out.collect("s1:" + ele + "=======s2:" + value);
                    }
                }
            }
        });

        ret.print();

        env.execute();
    }
}
