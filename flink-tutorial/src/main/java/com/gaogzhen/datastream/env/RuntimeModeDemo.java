package com.gaogzhen.datastream.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
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
public class RuntimeModeDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 流批一体：代码api一套，可以指定为批模式也可以指定为流模式，默认为STREAMING
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");
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
 * 1、默认 env.execute() 触发一个flink job
 *      一个main方法可以调用多个execute，但是没有意义，指定到第一个就会阻塞
 * 2、env.executeAsync() 异步触发，不阻塞
 * 3、yarn-application 集群，提交一次，集群里会有几个flink job？
 *  =》 取决于调用了几次executeAsync()
 *  => 对应application集群里，会有n个job
 *  => 对应jobmanager当做，会有n个jobMaster
 */