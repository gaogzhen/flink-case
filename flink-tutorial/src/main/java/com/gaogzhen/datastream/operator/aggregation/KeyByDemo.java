package com.gaogzhen.datastream.operator.aggregation;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class KeyByDemo {
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

        source.keyBy(WaterSensor::getId).print();

        env.execute();
    }
}
/**
 * keyby 分组与分区
 *  1 keyby 是对数据分组，保证相同key的数据在同一个分区
 *  2 分区：一个子任务可以理解为一个分区，一个分区可以有多个分组
 */