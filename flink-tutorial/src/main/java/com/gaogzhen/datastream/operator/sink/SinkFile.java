package com.gaogzhen.datastream.operator.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @author gaogzhen
 * @since 2023/12/9 15:06
 */
public class SinkFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 必须开启checkpoint，否则输出一直.inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                (GeneratorFunction<Long, String>) aLong -> "number:" + aLong,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        FileSink<String> fileSink = FileSink
                // 输出行式存储的文件，指定路径、指定编码
                .<String>forRowFormat(new Path("output/"), new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的配置：文件前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("gaogzhen-")
                                .withPartSuffix(".log")
                                .build()
                )
                // 安装目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 滚动策略：时间、大小，或的关系（满足其中一个条件即可）
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(new MemorySize(1024))
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .build()
                )
                .build();

        source.sinkTo(fileSink);

        env.execute();
    }
}
