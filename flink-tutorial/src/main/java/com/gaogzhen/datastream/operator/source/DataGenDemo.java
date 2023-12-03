package com.gaogzhen.datastream.operator.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2023/12/03 15:27
 */
public class DataGenDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从书籍生成器读取数据
        /**
         * 数据生成器，四个参数
         *  1 GeneratorFunction接口，需要实现，重写map映射方法，输入固定Long
         *  2 Long类型，自动生成的数字序列（从1开始）最大值
         *  3 RateLimiterStrategy 限速策略
         *  4 返回数据类型
         */
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) aLong -> "number:" + aLong,
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );

        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");


        source.print();

        env.execute();
    }
}

