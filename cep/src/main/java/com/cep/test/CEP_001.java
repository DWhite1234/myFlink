package com.cep.test;

import com.alibaba.fastjson.JSONObject;
import com.common.watermark.MyPeriodWatermark;
import com.alibaba.fastjson.JSON;
import com.cep.functions.CepPatternProcessFunction_001;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author zt
 */
@Slf4j
public class CEP_001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.1.2.245:8081,172.1.2.245:8082,172.1.2.245:8083")
                .setGroupId("test")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("111")
                .build();

//        KeyedStream<Person, String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka")
        KeyedStream<JSONObject, String> keyedStream = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data))
                .assignTimestampsAndWatermarks(
                        new WatermarkStrategy<JSONObject>() {
                            @Override
                            public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new MyPeriodWatermark(Duration.ofMillis(10));
                            }
                        }.withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getDate("ts").getTime();
                                    }
                                }
                        )
                )
                .keyBy(data -> (String) data.get("name"));


        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("msgEvent", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        log.info("触发第一次匹配");
                        return value.get("sex").equals("女");
                    }
                }).oneOrMore().greedy()
                .followedBy("txEvent")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        log.info("触发第2次匹配");
                        return value.get("sex").equals("男");
                    }
                })
                .within(Time.minutes(1));
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("out") {
        };
        CEP.pattern(keyedStream, pattern)
                .process(new CepPatternProcessFunction_001(outputTag))
                .print();
        env.execute();
    }
}
