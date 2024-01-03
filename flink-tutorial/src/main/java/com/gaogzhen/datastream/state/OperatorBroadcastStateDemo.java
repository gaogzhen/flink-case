package com.gaogzhen.datastream.state;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 针对每种传感器输出最高的3个水位值
 *
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        // 配置流
        DataStreamSource<String> configDs = env.socketTextStream("127.0.0.1", 8888);

        // 1、配置流广播
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        BroadcastStream<String> configBs = configDs.broadcast(broadcastMapState);

        // 2、数据流和广播后的配置流connect
        BroadcastConnectedStream<WaterSensor, String> sensorBcs = sensorDs.connect(configBs);

        // 3、调用process
        sensorBcs.process(
                        new BroadcastProcessFunction<WaterSensor, String, String>() {
                            /**
                             * 数据流处理方法
                             * @param value The stream element.
                             * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
                             *     querying the current processing/event time and updating the broadcast state. The context
                             *     is only valid during the invocation of this method, do not store it.
                             * @param out The collector to emit resulting elements to
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                // 5、上下文中获取广播状态（只读）
                                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                Integer threshold = broadcastState.get("threshold");
                                if (threshold == null) {
                                    threshold = 10;
                                }
                                Integer vc = value.getVc();
                                if (vc > threshold) {
                                    out.collect("当前水位值：" + vc + ",超过阈值：" + threshold);
                                }
                            }

                            /**
                             * 广播后配置流的处理方法
                             * @param value The stream element.
                             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
                             *     current processing/event time and updating the broadcast state. The context is only valid
                             *     during the invocation of this method, do not store it.
                             * @param out The collector to emit resulting elements to
                             * @throws Exception
                             */
                            @Override
                            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                                // 4、上下文获取广播状态，存入数据
                                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                                broadcastState.put("threshold", Integer.valueOf(value));
                            }
                        }
                )
                .print();

        env.execute();
    }
}