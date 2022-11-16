package com.kafka.offset;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author zt
 */
@Slf4j
public class KafkaSource extends RichParallelSourceFunction<String> {
    private KafkaConsumer<String, String> consumer;

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.1.2.215:8081,172.1.2.215:8082,172.1.2.215:8083");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    @Override
    public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
        TopicPartition topicPartition = new TopicPartition("kafkaTest", 0);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,0);
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
            if (record.value() != null) {
                log.debug("partition:{},offset:{}",record.partition(),record.offset());
                ctx.collect(record.value());
            }
        }
    }

    @Override
    public void cancel() {

    }
}
