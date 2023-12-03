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
public class SimpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(1);

        // 集合读取数据
        DataStreamSource<WaterSensor> source =env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 1),
                new WaterSensor("s2", 2L, 1),
                new WaterSensor("s3", 3L, 1)
        );

        KeyedStream<WaterSensor, String> sensorKS = source.keyBy(WaterSensor::getId);
        // sensorKS.sum("vc").print();
        // SingleOutputStreamOperator<WaterSensor> ret = sensorKS.max("vc");
        SingleOutputStreamOperator<WaterSensor> ret = sensorKS.maxBy("vc");

        env.execute();
    }
}
/**
 * 1 传位置索引适用于Tuple，POJO不行
 * 2 max/maxBy区别
 *  2.1 max 除了key和聚合字段，其他保留第一次的值
 *  2.2 maxBy 取聚合字段对应的一条
 */