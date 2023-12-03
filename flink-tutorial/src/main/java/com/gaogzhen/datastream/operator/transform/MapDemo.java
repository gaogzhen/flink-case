package com.gaogzhen.datastream.operator.transform;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 集合读取数据
        DataStreamSource<WaterSensor> source =env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s3", 1L, 1)
        );

        SingleOutputStreamOperator<String> map = source.map(WaterSensor::getId);
        map.print();

        env.execute();
    }
}
