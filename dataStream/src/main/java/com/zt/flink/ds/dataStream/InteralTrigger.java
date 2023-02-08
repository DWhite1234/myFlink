package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.zt.flink.ds.trigger.MyIntervalTrigger;
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

public class InteralTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                                    @Override
                                    public long extractTimestamp(Person element, long recordTimestamp) {
                                        return element.getTs().getTime();
                                    }
                                })
                )
                .keyBy(Person::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                /**
                 * ContinuousEventTimeTrigger:在窗口中划分一个小窗口
                 */
                .trigger(new MyIntervalTrigger())
                .process(new ProcessWindowFunction<Person, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Person, String, String, TimeWindow>.Context context, Iterable<Person> elements, Collector<String> out) throws Exception {
                        int sum = 0;
                        for (Person element : elements) {
                            sum += element.getMoney();
                        }
                        out.collect("金额:"+sum);
                    }
                })
                .print();

        env.execute();
    }
}
