package com.gaogzhen.datastream.operator.aggregation;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 集合读取数据
        DataStreamSource<WaterSensor> source =env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 1),
                new WaterSensor("s2", 2L, 1),
                new WaterSensor("s3", 3L, 1)
        );

        KeyedStream<WaterSensor, String> sensorKS = source.keyBy(WaterSensor::getId);
        SingleOutputStreamOperator<WaterSensor> reduce = sensorKS.reduce((k1, k2) ->
                {
                    System.out.println(k1);
                    System.out.println(k2);
                    return new WaterSensor(k1.getId(), k1.getTs(), k1.getVc() + k2.getVc());
                }
        );
        reduce.print();


        env.execute();
    }
}
/**
 * 1 keyBy 之后调用
 * 2 输入类型 = 输出类型
 * 3 每个key的第一条数据来的时候不会执行reduce方法，直接输出
 * 4 reduce方法中两个参数：
 *  value1：之前的计算结果，存状态
 *  value2：新数据
 */