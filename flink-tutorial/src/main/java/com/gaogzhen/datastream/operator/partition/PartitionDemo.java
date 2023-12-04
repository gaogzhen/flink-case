package com.gaogzhen.datastream.operator.partition;

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
public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(2);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        // shuffle 随机分区：random.nextInt(下游算子并行度);
        // socketDS.shuffle().print();

        // rebalance 轮询：nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels
        // 初始从下游并行度中随机选择一个，然后顺序开始轮询
        // 如果是数据倾斜场景，调用rebalance可以解决
        // socketDS.rebalance().print();

        // rescale 缩放轮询：实现轮询，局部组队，比rebalance更高效
        // socketDS.rescale().print();

        // broadcast 广播：发送给下游所有的子任务
        // socketDS.broadcast().print();

        // global 全局：全部发往下游第一个并行子任务
        // socketDS.global().print();

        // keyBy ：按照key发送，相同key发送到相同的子任务
        // socketDS.keyBy(key -> key).print();
        // forward：one-to-one 一对一分区器
        // socketDS.forward().print();

        env.execute();
    }
}
/**
 * 1、算子之间的关系：
 */