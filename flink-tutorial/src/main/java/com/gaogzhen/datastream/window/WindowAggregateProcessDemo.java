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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class WindowAggregateProcessDemo {
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
        // 2. 窗口函数: aggregate
        SingleOutputStreamOperator<String> ret = sensorWs.aggregate(
                new MyAgg(),
                new MyProcess()
        );

        ret.print();


        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {

        /**
         * 创建累加器
         * @return 累加器
         */
        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }


        /**
         * 聚合逻辑
         * @param value 输入值
         * @param accumulator 累加器
         * @return 聚合结果
         */
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法, value=" + value);
            return accumulator + value.getVc();
        }

        /**
         * 获取输出结果
         * @param accumulator 累加器
         * @return 结果
         */
        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            // 只有回话窗口用到
            System.out.println("调用merge方法");
            return null;
        }
    }

    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {


        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStart = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss.SSS");
            long count = elements.spliterator().estimateSize();
            out.collect("调用process，key=" + s + "的窗口[" + windowStart + "," + windowEnd + "]包含" + count + "条数据==>" + elements);
        }
    }
}
