package com.gaogzhen.datastream.env;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author: Administrator
 * @createTime: 2023/11/20 21:50
 */
public class EnvDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        DataStreamSource<String> lineDSS = env.socketTextStream("node1", 7777);
        // 2. 读取文件
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1);


        // 6. 打印
        sum.print();
        // 7. 执行
        env.execute();
    }
}
/**
 * 1、算子之间的关系：
 */