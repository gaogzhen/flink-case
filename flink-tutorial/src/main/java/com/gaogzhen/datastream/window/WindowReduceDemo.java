package com.gaogzhen.datastream.window;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        KeyedStream<WaterSensor, String> sensorKS = sensorDs.keyBy(WaterSensor::getId);
        // 1. 窗口分配器

        WindowedStream<WaterSensor, String, TimeWindow> sensorWs = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 2. 窗口函数
        /**
         * 窗口reduce
         * 1、相同的key的第一条数据来的时候，不会调用reduce方法
         * 2、增量聚合：来一条数据，就会计算一次，不会输出
         * 3、窗口触发的时候，才会输出
         */
        SingleOutputStreamOperator<WaterSensor> reduce = sensorWs.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("调用reduce, value=" + value1 + ",value2=" + value2);
                        return new WaterSensor(value1.getId(), value1.getTs(), value1.getVc() + value2.getVc());
                    }
                }
        );

        reduce.print();


        env.execute();
    }
}
