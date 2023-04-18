package com.zt.flink.ds.ck;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class LocalCk {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path","/Users/zhongtao/disk/develop/ck/20230417/9c5e848b70a477acf9b19dd1d6e529ca/chk-1");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhongtao/disk/develop/ck/20230417/");
//        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(Person::getName)
                .process(new KeyedProcessFunction<String, Person, Person>() {
                    private ValueState<String> valueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("test", String.class);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(Person value, KeyedProcessFunction<String, Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                        log.info("data:{}", valueState.value());
                        valueState.update("1");
                        out.collect(value);
                    }
                }).name("test-state").uid("test-state")
                .print();


        env.execute();
    }
}
