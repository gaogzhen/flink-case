package com.gaogzhen.datastream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gaogzhen
 * @since 2023/12/10 15:30
 */
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.socketTextStream("127.0.0.1", 7777)
                .map(new MyCountMapFunction())
                .print();


        env.execute();
    }

    // 1.实现CheckPointedFunction接口
    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        /**
         * 处理数据
         * @param value The input value.
         * @return
         * @throws Exception
         */
        public Long map(String value) throws Exception {
            return ++count;
        }

        /**
         * 2.把本地变量拷贝到算子状态中
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            // 2.1 获取算子状态
            state.clear();
            state.add(count);
        }

        /**
         * 3.初始化本地变量：从状态中获取值赋值给本地变量，每个子任务调用一次
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState...");
            // 3.1 从上下文初始化算子状态
            state = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>("state", Types.LONG)
            );
            // 3.2 从算子状态中获取赋值给本地变量
            if (context.isRestored()) {
                for (Long c : state.get()) {
                    count += c;
                }
            }
        }
    }
}

/**
 * 算子状态中，List与UnionList的区别：并行度改变时，怎么重新分配状态
 * 1、list状态：合并后，轮询分给新的并行子任务
 * 2、unionList专题：合并后，给每个并行子任务一份完整的
 */