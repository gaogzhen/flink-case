package com.gaogzhen.datastream.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.Properties;

/**
 * @author: Administrator
 * @createTime: 2024/02/06 10:01
 */
public class FlinkKafkaProducer1 {
    public static void main(String[] args) throws Exception {
        // 1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        // 2 装备数据源
        ArrayList<String> wordlist = new ArrayList<>();
        wordlist.add("flink");
        wordlist.add("kafka");
        wordlist.add("java");
        DataStreamSource<String> streamSource = env.fromCollection(wordlist);
        // 3 添加数据源
        // kafka生产者
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("first", new SimpleStringSchema(), properties);
        streamSource.addSink(kafkaProducer);
        // 4 执行代码
        env.execute();
    }
}
