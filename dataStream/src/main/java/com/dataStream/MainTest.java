package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author zt
 */
@Slf4j
public class MainTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(data -> data.getName())
                .process(new KeyedProcessFunction<String, Person, Object>() {
                    @Override
                    public void processElement(Person value, KeyedProcessFunction<String, Person, Object>.Context ctx, Collector<Object> out) throws Exception {
                        out.collect(value);
                    }
                })
                .print();


        env.execute();
    }
}
