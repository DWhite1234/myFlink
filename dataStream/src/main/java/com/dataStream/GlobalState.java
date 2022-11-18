package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.functions.Test001;
import com.functions.Test003;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author zt
 */

public class GlobalState {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.setString("execution.savepoint.path", "file:///Users/zhongtao/disk/develop/ck/4e460fb48f719aef6ad9130875da7275/chk-1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhongtao/disk/develop/ck");
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        ListStateDescriptor<Person> listState = new ListStateDescriptor<>("listState", Person.class);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                            @Override
                            public long extractTimestamp(Person element, long recordTimestamp) {
                                return element.getTs().getTime();
                            }
                        }))
                .keyBy(data -> data.getName())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(CountTrigger.of(1))
                .process(new Test001(listState))
                .map(new Test003(listState))
                .print();

        env.execute();
    }
}
