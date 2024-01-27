package com.gaogzhen.datastream.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class SavePointDemo {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);


        // 代码中用到hdfs，需要导入hadoop依赖，指定访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 检查点配置
        // 1.启用检查点：默认barrier对齐，精确一次，周期5s
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.指定检查点的位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://node1:8020/chk");
        // 3.超时时间：默认10分钟
        checkpointConfig.setCheckpointTimeout(60000L);
        // 4.最大并发检查点数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 5.最小时间间隔：上一轮checkpoint结束到下一轮checkpoint开始之间的间隔；设置>0，并发数为1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000L);
        // 6.取消作业时，checkpoint的数据保留在外部系统策略
        // DELETE_ON_CANCELLATION 主动取消会删除存在外部系统的chk-xx目录；如果是程序突然挂掉，不会删除
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 7.运行checkpoint连续失败的次数，默认0，表示失败一次，job失败
        checkpointConfig.setTolerableCheckpointFailureNumber(10);


        // 开启非对齐检查点
        // 开启条件：checkpoint模式为精准一次，最多并发1
        checkpointConfig.enableUnalignedCheckpoints();
        // 开启非对齐检查点才生效：默认0，表示一开始就直接用非对齐的检查点
        // 如果大于0，一开始用对齐的检查点（barrier）,对齐的时间超过这个参数，自动切换成非对齐的检查点
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        env.socketTextStream("node2", 7777)
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(",");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).uid("wc-flatmap")
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(v -> v.f0)
                .sum(1).uid("wc-sum")
                .print();


        env.execute();
    }
}
