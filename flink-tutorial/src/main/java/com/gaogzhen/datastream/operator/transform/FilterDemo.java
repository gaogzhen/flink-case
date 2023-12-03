package com.gaogzhen.datastream.operator.transform;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.FilterFunctionImpl;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 集合读取数据
        DataStreamSource<WaterSensor> source =env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s3", 1L, 1)
        );

        SingleOutputStreamOperator<WaterSensor> ret = source.filter(new FilterFunctionImpl("s1"));
        ret.print();

        env.execute();
    }
}
