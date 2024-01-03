package com.gaogzhen.datastream.state;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 检测每种传感器的水位值，如果连续的两个水位值只差超过10，就输出报警
 *
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class KeyedvalueStateDemo {
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
                    ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2. open方法中初始化状态
                        // 状态描述参数：1名称，唯一不重复；2存储的类型
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 1. 取出上一条的水位值
                        Integer lastVc = lastVcState.value();
                        if (lastVc == null) {
                            lastVc = 0;
                        }
                        // 2. 求差值绝对值，判断是否超过10
                        Integer currVc = value.getVc();
                        if (currVc == null) {
                            currVc = 0;
                        }
                        if (Math.abs(currVc - lastVc) > 10) {
                            out.collect("传感器ID：" + value.getId() + "====>当前水位值：" + currVc + ",上一条水位值：" + lastVc + "，差值超过10！");
                        }
                        // 3. 更新自己的水位值
                        lastVcState.update(currVc);
                    }
                })
                .print();

        env.execute();
    }
}