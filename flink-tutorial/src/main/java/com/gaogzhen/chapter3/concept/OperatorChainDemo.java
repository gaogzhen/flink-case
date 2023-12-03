package com.gaogzhen.chapter3.concept;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author: Administrator
 * @createTime: 2023/11/20 21:50
 */
public class OperatorChainDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        // env.disableOperatorChaining();

        DataStreamSource<String> lineDSS = env.socketTextStream("node1", 7777);
        // 2. 读取文件
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = lineDSS
                // .disableChaining()
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(" ")).forEach(words::collect);
                })
                // .disableChaining()
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
 *  1.1 one-to-one
 *  1.2 distribution
 * 2、算子链合并：
 *  1.1 forwarding 一对一
 *  1.2 并行度相同
 * 3、禁用算子链：
 *  1.1 全局禁用
 *  1.2 单个算子禁用：算子不会参与前后的算子链合并
 *  1.3 startNewChain : 算子不会前面算子链合并
 */