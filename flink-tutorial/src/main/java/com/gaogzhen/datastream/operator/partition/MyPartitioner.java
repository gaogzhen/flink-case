package com.gaogzhen.datastream.operator.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author gaogzhen
 * @since 2023/12/4 07:53
 */
public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String s, int i) {
        return Integer.parseInt(s) % i;
    }
}
