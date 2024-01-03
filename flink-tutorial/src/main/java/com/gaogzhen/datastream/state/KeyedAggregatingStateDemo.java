package com.gaogzhen.datastream.state;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 计算每种传感器的平均水位
 *
 * @author gaogzhen
 * @since 2024/01/01
 */
public class KeyedAggregatingStateDemo {
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

                    AggregatingState<Integer, Double> vcAvg;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcAvg = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "vcAvg",
                                new MyAgg(),
                                Types.TUPLE(Types.INT, Types.DOUBLE)
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        // 1.获取当前水位值
                        Integer currVc = value.getVc();
                        if (currVc == null) {
                            currVc = 0;
                        }
                        // 2.累加计算
                        vcAvg.add(currVc);

                        out.collect("传感器id：" + value.getId() + ",水位值之和：" + vcAvg.get());
                    }
                })
                .print();

        env.execute();
    }

    public static class MyAgg implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
            return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return accumulator.f0 * 1D / accumulator.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


}