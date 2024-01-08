package com.gaogzhen.datastream.checkpoint;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class CheckConfigDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);

        // 检查点配置
        // 1.启用检查点：默认barrier对齐，精确一次，周期5s
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 2.指定检查点的位置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://node1:8020/chk");
        // 3.超时时间：默认10分钟
        checkpointConfig.setCheckpointTimeout(60000L);
        // 4.最大并发检查点数量
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 5.最小时间间隔：上一轮checkpoint结束到下一轮checkpoint开始之间的间隔；设置>0，并发数为1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000L);
        // 6.取消作业时，checkpoint的数据保留在外部系统策略
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        // 7.运行checkpoint连续失败的次数，默认0，表示失败一次，job失败
        checkpointConfig.setTolerableCheckpointFailureNumber(10);


        env.socketTextStream("127.0.0.1", 7777)
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(",");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(v -> v.f0)
                .sum(1)
                .print();


        env.execute();
    }
}
