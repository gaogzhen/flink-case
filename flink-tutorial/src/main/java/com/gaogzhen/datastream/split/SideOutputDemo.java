package com.gaogzhen.datastream.split;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import com.gaogzhen.datastream.operator.partition.MyPartitioner;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author: Administrator
 * @createTime: 2023/11/20 21:50
 */
public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(2);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        /**
         * 创建OutTag对象
         *  参数1：标签名
         *  参数2：放入侧输出流中的数据类型
         */
        OutputTag<WaterSensor> tag1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> tag2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = sensorDs.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    /**
                     *  ctx.output
                     *      参数1：输出标签
                     *      参数2：数据
                     */
                    ctx.output(tag1, value);
                } else if ("s2".equals(id)) {

                    ctx.output(tag2, value);
                } else {
                    // 非s1、s2数据放入主流
                    out.collect(value);
                }
            }
        });

        // 打印主流
        process.print("主流");
        // 打印侧输出流s1
        SideOutputDataStream<WaterSensor> sideOutput1 = process.getSideOutput(tag1);
        sideOutput1.printToErr("s1");
        // 打印侧输出流s2
        SideOutputDataStream<WaterSensor> sideOutput2 = process.getSideOutput(tag2);
        sideOutput2.printToErr("s2");

        env.execute();
    }
}

/**
 * 使用侧输出流
 *  1 定义process算子
 *  2 定义OutTag对象
 *  3 调用ctx.output()
 *  4 通过主流获取侧流
 */