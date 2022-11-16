package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author zt
 * 实例化次数测试
 */
@Slf4j
public class InstantiationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(Person::getName)
                .process(new KeyedProcessFunction<String, Person, Person>() {
                    private ValueState<String> valueState;
                    private Integer count;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("va", String.class);
                        valueState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Person value, KeyedProcessFunction<String, Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                        if (valueState.value()==null) {
                            count = 0;
                            valueState.update(count+"");
                        }
                        count++;
                        log.info("当前key:{},当前count:{}",ctx.getCurrentKey(),count);
                    }
                });


        env.execute();
    }
}
