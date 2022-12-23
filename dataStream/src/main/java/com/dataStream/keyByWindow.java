package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import java.time.Duration;

/**
 * @author zt
 */
@Slf4j
public class keyByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                })
                )
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .trigger(CountTrigger.of(1))
                .aggregate(new AggregateFunction<Person, Person, Person>() {
                    @Override
                    public Person createAccumulator() {
                        return new Person();
                    }

                    @Override
                    public Person add(Person value, Person accumulator) {

                        log.info("acc:{}",accumulator);
                        accumulator.setAge(value.getAge());
                        log.info("acc:{}",accumulator);
                        return value;
                    }

                    @Override
                    public Person getResult(Person accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Person merge(Person a, Person b) {
                        return null;
                    }
                });


        env.execute();
    }
}
