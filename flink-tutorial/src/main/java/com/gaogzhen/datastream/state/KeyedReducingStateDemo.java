package com.gaogzhen.datastream.state;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * 计算每种传感器水位值之和
 *
 * @author gaogzhen
 * @since 2024/01/01
 */
public class KeyedReducingStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                // 1.1 指定watermark生成：乱序，有等待时间
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                // 1.2 指定时间戳分配器，从数据中提取
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L)

                );


        sensorDs.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    // 1. 定义状态
                    ReducingState<Integer> vcSum;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2. open方法中初始化状态
                        vcSum = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "vcSum",
                                (v1,v2) -> v1 + v2,
                                Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 1. 获取水位值对应的计数
                        Integer currVc = value.getVc();
                        if (currVc == null) {
                            currVc = 0;
                        }
                       // 2. 水位值求和
                        vcSum.add(currVc);
                        out.collect("传感器id：" + value.getId() + ",水位值之和：" + vcSum.get());
                    }
                })
                .print();

        env.execute();
    }
}