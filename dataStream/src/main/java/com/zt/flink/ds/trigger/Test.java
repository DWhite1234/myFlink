package com.zt.flink.ds.trigger;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.common.config.BaseApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * @author zt
 */
@Slf4j
public class Test extends BaseApplication {
    public static void main(String[] args) throws Exception {
        configureStreamExecutionEnvironment(args);
        env.setParallelism(1);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.1.2.215:8081,172.1.2.215:8082,172.1.2.215:8083")
                .setTopicPattern(Pattern.compile(String.format("[a-zA-Z]*[\\.]%s|%s", "MESSAGE-STATISTIC", "MESSAGE-STATISTIC")))
                .setGroupId("test-001")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // env.socketTextStream("localhost", 999)
        env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka")
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                })
                                .withIdleness(Duration.ofMinutes(1))
                )
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .trigger(new MyIntervalTrigger())
                .process(new ProcessWindowFunction<Person, Person, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Person, Person, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {
                        for (Person element : elements) {
                            log.info("窗口1触发:watermark:{}",context.currentWatermark());
                            out.collect(element);
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyWindowIntervalTrigger())
                .evictor(new Evictor<Person, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Person>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Person>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
                        Iterator<TimestampedValue<Person>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            iterator.next();
                            iterator.remove();
                        }
                    }
                })
                .process(new ProcessAllWindowFunction<Person, Person, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Person, Person, TimeWindow>.Context context, Iterable<Person> elements, Collector<Person> out) throws Exception {
                        for (Person element : elements) {
                            out.collect(element);
                        }
                    }
                })
                .process(new ProcessFunction<Person, Person>() {
                    @Override
                    public void processElement(Person value, ProcessFunction<Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                        log.info("当前参与排序的数据为:{}",value.getTs());
                    }
                })
                .addSink(new SinkFunction<Person>(){
                    @Override
                    public void invoke(Person value, Context context) throws Exception {
                        log.info("排序数据:{}",value.getTs());
                    }
                });

        env.execute();
    }
}
