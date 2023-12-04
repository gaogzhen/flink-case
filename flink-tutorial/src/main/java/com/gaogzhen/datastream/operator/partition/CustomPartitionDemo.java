package com.gaogzhen.datastream.operator.partition;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/11/20 21:50
 */
public class CustomPartitionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        socketDS.partitionCustom(new MyPartitioner(), k -> k).print();

        env.execute();
    }
}