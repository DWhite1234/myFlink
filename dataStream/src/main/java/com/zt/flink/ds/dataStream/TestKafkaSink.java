package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class TestKafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String url = "172.1.2.245:8081,172.1.2.215:8082,172.1.2.215:8083";
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(url)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopicSelector(new TopicSelector<String>() {
                                    @Override
                                    public String apply(String element) {
                                        if (element.equals(url)) {
                                            return "111";
                                        }
                                        return null;
                                    }
                                })
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()

                )
                .build();


        env.socketTextStream("localhost", 999)
                .map(data -> data)
                .sinkTo(kafkaSink);


        env.execute();
    }
}
