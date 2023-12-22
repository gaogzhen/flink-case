package com.gaogzhen.datastream.window;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        KeyedStream<WaterSensor, String> sensorKS = sensorDs.keyBy(WaterSensor::getId);
        // 1. 窗口分配器

        WindowedStream<WaterSensor, String, TimeWindow> sensorWs = sensorKS
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(10))); // 滚动窗口：窗口长度10s
                // .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5) )); // 滑动窗口：窗口长度10s，步长5s
                // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); // 会话窗口：间隔5s
                .window(ProcessingTimeSessionWindows.withDynamicGap(ws -> ws.getTs() * 1000)); // 会话窗口：动态间隔
        // 2. 窗口函数: aggregate
        SingleOutputStreamOperator<String> ret = sensorWs.process(

                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     *
                     * @param s 分组的key
                     * @param context 上下文
                     * @param elements 存的数据
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("调用process，key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据==>" + elements);
                    }
                }
        );

        ret.print();


        env.execute();
    }
}
