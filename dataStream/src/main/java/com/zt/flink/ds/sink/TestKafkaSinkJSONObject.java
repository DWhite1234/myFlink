package com.zt.flink.ds.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import java.nio.charset.StandardCharsets;

/**
 * @author zt
 */

public class TestKafkaSinkJSONObject {
    public static void main(String[] args) {
        /*
         主要是方便了根据数据内容发往不同的主题的适配
         */
        KafkaSink<JSONObject> build = KafkaSink.<JSONObject>builder()
                .setBootstrapServers("")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopicSelector(new TopicSelector<JSONObject>() {
                                    @Override
                                    public String apply(JSONObject jsonObject) {
                                        return null;
                                    }
                                })
                                .setValueSerializationSchema(new SerializationSchema<JSONObject>() {
                                    @Override
                                    public byte[] serialize(JSONObject element) {
                                        return JSON.toJSONString(element).getBytes(StandardCharsets.UTF_8);
                                    }
                                })
                                //使用kafakSink 多并行度的时候需要搭配keyby使用
                                .setPartitioner(new FlinkFixedPartitioner<>())
                                .build()
                ).build();
    }
}
