package com.gaogzhen.datastream.operator.transform;

import com.gaogzhen.datastream.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:21
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // env.setParallelism(1);

        // 集合读取数据
        DataStreamSource<Integer> source =env.fromElements(1,2,3,4);

        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println(
                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask() +
                        ", 子任务名称=" + runtimeContext.getTaskNameWithSubtasks() +
                                ",调用open()"
                );
            }

            @Override
            public void close() throws Exception {
                super.close();
                RuntimeContext runtimeContext = getRuntimeContext();
                System.out.println(
                        "子任务编号=" + runtimeContext.getIndexOfThisSubtask() +
                                ", 子任务名称=" + runtimeContext.getTaskNameWithSubtasks() +
                                ",调用close()"
                );
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        });
        map.print();

        env.execute();
    }
}

/**
 * RichXXXFunction：富函数
 * 1 多了生命周期方法
 *  1.1 open() ：每个子任务，在启动时，调用一次
 *  1.2 close()：每个子任务，在结束时，调用一次
 *      如果flink程序异常挂掉，不会close
 *      正常cancel任务，可以close
 * 2 多了运行时上下文
 *  可以获取运行时环境信息，比如：子任务编号、名称等
 */