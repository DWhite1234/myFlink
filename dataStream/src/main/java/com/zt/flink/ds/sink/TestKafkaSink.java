package com.zt.flink.ds.sink;

import com.alibaba.fastjson.JSON;
import com.common.config.BaseApplication;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

/**
 * @author zt
 */

public class TestKafkaSink extends BaseApplication {
    public static void main(String[] args) throws Exception {
        configureStreamExecutionEnvironment(args);
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("test2")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .setPartitioner(new FlinkFixedPartitioner<>())
                                .build()
                ).build();

        /** 如果要使用kafkaSink的多并行度,则需要keyby 一个并行度负责写入一个分区 */
        env.socketTextStream("localhost",888)
                .keyBy(data -> data)
                .sinkTo(kafkaSink).setParallelism(3);

        env.execute();

    }
}
