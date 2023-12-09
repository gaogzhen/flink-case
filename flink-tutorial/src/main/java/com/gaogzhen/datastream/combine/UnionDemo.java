package com.gaogzhen.datastream.combine;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> ds2 = env.fromElements(12, 2222, 1113, 433);
        DataStreamSource<String> ds3 = env.fromElements("22", "999", "222", "999");

        /**
         * union：合并数据流
         *  1 流的数据类型必须一致
         *  2 一次可以合并多条流
         */
        DataStream<Integer> ret = ds1.union(ds2, ds3.map(Integer::valueOf));

        ret.print();

        env.execute();
    }
}
