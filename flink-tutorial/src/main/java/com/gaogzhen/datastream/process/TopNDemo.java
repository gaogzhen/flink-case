package com.gaogzhen.datastream.process;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class TopNDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("127.0.0.1", 7777)
                .map(new WaterSensorMapFuntion());

        // 指定watermark 策略
        // 1. 定义watermark策略
        WatermarkStrategy<WaterSensor> watermarkStrategy = WatermarkStrategy
                // 1.1 指定watermark生成：乱序，有等待时间
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 1.2 指定时间戳分配器，从数据中提取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000L);

        SingleOutputStreamOperator<WaterSensor> sensorWithWatermark = sensorDs.assignTimestampsAndWatermarks(watermarkStrategy);

        // 1. 按照vc分组、开窗、聚合（增量聚合、全量打标签）
        // 开窗聚合之后就是普通的流，没有窗口信息，需要自己打上窗口标记
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> windowAgg = sensorWithWatermark.keyBy(WaterSensor::getVc)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new VcCountAgg(),
                        new WindowResult()
                );
        // 2. 按照窗口标签keyBy，排序，取topN
        windowAgg.keyBy(data -> data.f2)
                        .process(new TopN(2))
                                .print();


        env.execute();
    }

    public static class VcCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    /**
     * 泛型如下：
     * IN ： 增量函数的输出，count值， Integer
     * OUT：输出列席Tuple3(vc,count,windowEnd)
     * KEY：vc类型Integer
     * WINDOW：窗口类型
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow> {

        /**
         * @param key      The key for which this window is evaluated.
         * @param context  The context in which the window is being evaluated.
         * @param elements The elements in the window being evaluated.
         * @param out      A collector for emitting elements.
         * @throws Exception
         */
        @Override
        public void process(Integer key, ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>, Integer, TimeWindow>.Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
            // 迭代器里面只有一条数据
            Integer count = elements.iterator().next();
            long end = context.window().getEnd();
            out.collect(Tuple3.of(key, count, end));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String> {

        private Map<Long, List<Tuple3<Integer, Integer, Long>>> dataListMap;
        // 要取的top的数量
        private int threshold;

        public TopN(int threshold) {
            this.threshold = threshold;
            this.dataListMap = new HashMap<>();
        }

        @Override
        public void processElement(Tuple3<Integer, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            // 进入这个方法，只有一条数据，要排序，得数据到齐才行；容器存储，不同窗口分开
            // 1 存入容器
            Long windowEnd = value.f2;
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.getOrDefault(windowEnd, new ArrayList<>());
            dataList.add(value);
            dataListMap.put(windowEnd, dataList);
            // 2 注册一个定时器，windowEnd + 1ms
            // 同一个窗口范围，应该同时输出，然后一条一条调用processElement，只需要延迟1ms即可
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(windowEnd + 10L);

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 定时器触发，同一窗口范围的计算结果排序
            // 1 排序
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<Integer, Integer, Long>> dataList = dataListMap.get(windowEnd);
            List<Tuple3<Integer, Integer, Long>> retList = dataList.stream()
                    .sorted((o1, o2) -> o2.f1 - o1.f1)
                    .limit(threshold)
                    .collect(Collectors.toList());
            // 2 取topN
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("TOP\tvc\tcount\twindowEnd\n");
            stringBuilder.append("================\n");
            for (int i = 0; i < retList.size(); i++) {
                Tuple3<Integer, Integer, Long> ret = retList.get(i);
                stringBuilder.append((i+1) +"\t" + ret.f0 + "\t" + ret.f1 + "\t\t" + ret.f2 + "\n");
                stringBuilder.append("================\n");
            }
            dataList.clear();
            out.collect(stringBuilder.toString());
        }
    }
}
