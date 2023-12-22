package com.gaogzhen.datastream.window;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        KeyedStream<WaterSensor, String> sensorKS = sensorDs.keyBy(WaterSensor::getId);
        // 1. 窗口分配器

        WindowedStream<WaterSensor, String, GlobalWindow> sensorWs = sensorKS
                // .countWindow(3); // 滚动窗口：窗口长度3
                .countWindow(3, 1); // 滑动窗口：窗口长度3，步长1

        // 2. 窗口函数: aggregate
        SingleOutputStreamOperator<String> ret = sensorWs.process(

                new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
                    /**
                     *
                     * @param s 分组的key
                     * @param context 上下文
                     * @param elements 存的数据
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

                        long maxTs = context.window().maxTimestamp();

                        String maxTime = DateFormatUtils.format(maxTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("调用process，key=" + s + "的窗口最大时间" + maxTime + "，包含" + count + "条数据==>" + elements);
                    }
                }
        );

        ret.print();


        env.execute();
    }
}
