package com.cep.test;

import com.alibaba.fastjson.JSON;
import com.cep.functions.CepPatternProcessFunction_001;
import com.cep.functions.CepPatternProcessFunction_002;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import myCep.CEP;
import myCep.nfa.aftermatch.AfterMatchSkipStrategy;
import myCep.pattern.Pattern;
import myCep.pattern.conditions.SimpleCondition;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author zt
 */
@Slf4j
public class CEP_002 {
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
        SingleOutputStreamOperator<Person> operator = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(Person::getName)
                .process(new KeyedProcessFunction<String, Person, Person>() {
                    private Long timer;
                    private Person person1;
                    @Override
                    public void processElement(Person person, KeyedProcessFunction<String, Person, Person>.Context context, Collector<Person> collector) throws Exception {
                        if (timer!=null) {
                            context.timerService().deleteProcessingTimeTimer(timer);
                        }
                        timer = System.currentTimeMillis();
                        context.timerService().registerProcessingTimeTimer(timer+2000);
                        person1 = person;
                        collector.collect(person);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Person, Person>.OnTimerContext ctx, Collector<Person> out) throws Exception {
                        out.collect(person1);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person person, long l) {
                                        return person.getTs().getTime();
                                    }
                                })
                );


        Pattern<Person, Person> pattern = Pattern
                .<Person>begin("msgEvent", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<Person>() {
                    @Override
                    public boolean filter(Person value) throws Exception {
                        log.info("进入第一个条件,消息为:{}",value);
                        return value.getSex().equals("女");
                    }
                }).oneOrMore().greedy()
                .followedBy("txEvent")
                .where(new SimpleCondition<Person>() {
                    @Override
                    public boolean filter(Person person) throws Exception {
                        log.info("进入第二个条件,消息为:{}",person);
                        return person.getSex().equals("男");
                    }
                }).within(Time.seconds(2));
        OutputTag<Person> outputTag = new OutputTag<Person>("out") {
        };
        CEP.pattern(operator.keyBy(Person::getName), pattern)
                .process(new CepPatternProcessFunction_002(outputTag))
                .print();
        env.execute();
    }
}
