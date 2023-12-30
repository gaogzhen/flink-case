package com.gaogzhen.datastream.process;

import com.gaogzhen.datastream.bean.WaterSensor;
import com.gaogzhen.datastream.functions.WaterSensorMapFuntion;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class KeyedProcessTimerDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(1);
        // env.setParallelism(2);

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

        SingleOutputStreamOperator<String> process = sensorWithWatermark.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            /**
                             * 来一条数据调用一次
                             * @param value The input value.
                             * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
                             *     {@link TimerService} for registering timers and querying the time. The context is only
                             *     valid during the invocation of this method, do not store it.
                             * @param out The collector for returning result values.
                             * @throws Exception
                             */
                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                // 数据中提取的事件时间
                                Long timestamp = ctx.timestamp();

                                // 定时器
                                TimerService timerService = ctx.timerService();
                                // 注册定时器
                                // 事件时间
                                // timerService.registerEventTimeTimer(5000L);
                                // 处理时间
                                long processingTime = timerService.currentProcessingTime();
                                timerService.registerProcessingTimeTimer(processingTime + 5000L);
                                // 删除定时器
                                // 删除事件时间定时器
                                // timerService.deleteEventTimeTimer();
                                // 删除处理时间定时器
                                // timerService.deleteProcessingTimeTimer();
                                // 当前系统时间
                                // long processingTime = timerService.currentProcessingTime();
                                // 当前的watermark
                                // long currentWatermark = timerService.currentWatermark();
                                String currentKey = ctx.getCurrentKey();
                                // System.out.println("当前的key:" + currentKey + ",当前时间：" + timestamp + ",注册了一个5s的定时器");
                                System.out.println("当前的key:" + currentKey + ",当前时间：" + processingTime + ",注册了一个5s后的定时器");
                            }

                            /**
                             * 时间进展到定时器注册的时间，调用该方法
                             * @param timestamp The timestamp of the firing timer.
                             * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
                             *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
                             *     registering timers and querying the time. The context is only valid during the invocation
                             *     of this method, do not store it.
                             * @param out The collector for returning result values.
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                String currentKey = ctx.getCurrentKey();
                                System.out.println("当前的key:" + currentKey + ",现在时间：" + timestamp + ",定时器触发");
                            }
                        }


                );

        process.print();


        env.execute();
    }
}

/**
 * 定时器
 * 1. keyed 采用
 * 2. 事件时间定时器， 通过watermark触发
 *  2.1 watermark >= 当前最大等待事件时间 - 等待时间 - 1ms
 * 3. 在process中获取当前watermark，显示的是上一次的watermark（以为process环没有接收到当前数据对应生成的watermark）
 *
 */