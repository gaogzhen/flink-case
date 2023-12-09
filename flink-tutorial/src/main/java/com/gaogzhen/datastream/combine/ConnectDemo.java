package com.gaogzhen.datastream.combine;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author gaogzhen
 * @since 2023/12/4 21:21
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> ds1 = env.socketTextStream("127.0.0.1", 7777)
                .map(Integer::parseInt);
        SingleOutputStreamOperator<String> ds3 = env.socketTextStream("127.0.0.1", 8888);

        ConnectedStreams<Integer, String> connect = ds1.connect(ds3);

        SingleOutputStreamOperator<String> ret = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "数字流：" + value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return "字母流：" + value;
            }
        });

        ret.print();

        env.execute();
    }
}
