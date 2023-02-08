package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author zt
 */

@Slf4j
public class BoundedOutOfTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person person, long l) {
                                        return person.getTs().getTime();
                                    }
                                })
                )
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .process(new ProcessWindowFunction<Person, Person, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Person, Person, String, TimeWindow>.Context context, Iterable<Person> iterable, Collector<Person> collector) throws Exception {
                        log.info("窗口数据:{}",iterable);
                    }
                })
                .print();

        env.execute();
    }
}
