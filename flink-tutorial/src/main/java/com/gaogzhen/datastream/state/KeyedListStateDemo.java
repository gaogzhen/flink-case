package com.gaogzhen.datastream.state;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 针对每种传感器输出最高的3个水位值
 *
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class KeyedListStateDemo {
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
                    ListState<Integer> top3;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 2. open方法中初始化状态
                        // 状态描述参数：1名称，唯一不重复；2存储的类型
                        top3 = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 1. 水位值列表添加新的水位值

                        Integer currVc = value.getVc();
                        if (currVc == null) {
                            currVc = 0;
                        }
                        // 2. 排序，取前3个
                        top3.add(currVc);
                        Iterable<Integer> vcs = top3.get();
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcs) {
                            vcList.add(vc);
                        }
                        List<Integer> ret = vcList.stream()
                                .sorted(((o1, o2) -> o2 - o1))
                                .limit(3)
                                .collect(Collectors.toList());
                        out.collect("传感器id：" + value.getId() + "===> 最大的3个水位值=" + ret);
                        // 3. 更新自己的水位值列表
                        top3.update(ret);
                    }
                })
                .print();

        env.execute();
    }
}