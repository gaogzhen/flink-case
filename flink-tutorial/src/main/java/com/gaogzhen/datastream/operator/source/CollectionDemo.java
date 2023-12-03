package com.gaogzhen.datastream.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 集合读取数据
        DataStreamSource<Integer> source =
                // env.fromCollection(Arrays.asList(1, 22, 33, 4));
                env.fromElements(1,2,5);

        source.print();

        env.execute();
    }
}
