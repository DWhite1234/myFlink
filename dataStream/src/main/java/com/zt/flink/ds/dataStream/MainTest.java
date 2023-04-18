package com.zt.flink.ds.dataStream;

import com.zt.flink.java.utils.RandomUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */
@Slf4j
public class MainTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",999)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        log.info("当前随机数为:{}",RandomUtils.getRange(0, 10));
                        System.out.println("当前随机数为:"+RandomUtils.getRange(0, 10));
                        return value;
                    }
                })
                .print();

//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("172.1.2.215:8081,172.1.2.215:8082,172.1.2.215:8083")
//                .setTopics("111")
//                .setGroupId("test")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();

//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka")
//                .print();

        env.execute();
    }
}
