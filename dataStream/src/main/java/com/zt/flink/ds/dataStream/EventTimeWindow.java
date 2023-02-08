package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
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

/*
1.不开超时,只开延迟
    水印>窗口最大时间就会触发计算,同时后续再有数据,就会被抛弃
2.开启超时,开启延迟
    1.在窗口长度+乱序时间的数据之前不会触发计算
    2.在窗口长度+乱序时间的数据到时会触发计算
    3.在窗口长度+乱序时间之后到来的每个属于当前窗口的数据都会触发一次计算
    4.在窗口长度+乱序时间+超时时间到达时关闭对应的窗口,之后再来当前窗口的数据也不会触发计算
    5.在窗口长度+乱序时间+超时时间之后再来当前窗口的数据也不会触发计算
举例:
    窗口长度:10,乱序时间:5,超时时间:2
    数据顺序:
        1:  1
        2:  7
        3:  10
        4:  15    ->符合条件2触发一次计算
        5:  09    ->符合条件3触发一次计算
        6:  08    ->符合条件3触发一次计算
        7:  17    ->符合条件4,不触发计算,关闭窗口
        8:  08    ->符合条件5,不触发计算
 */
public class EventTimeWindow {
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
//                .trigger(new MyIntervalTrigger())
                .allowedLateness(Time.seconds(3))
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
