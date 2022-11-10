package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.spring.utils.SpelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

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
                    }
                })
                .print();


        env.execute();
    }
}
