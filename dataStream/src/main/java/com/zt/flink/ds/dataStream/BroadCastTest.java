package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.zt.flink.ds.io.Test002;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class BroadCastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        KeyedStream<Person, String> keyedStream = env.socketTextStream("localhost", 999)
                .map(data -> JSON.parseObject(data, Person.class))
                .keyBy(data -> data.getName());


        SingleOutputStreamOperator<Person> broad = env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, Person.class));
        MapStateDescriptor<String, Person> broadDes = new MapStateDescriptor<>("broad", String.class, Person.class);
        BroadcastStream<Person> broadcastStream = broad.broadcast(broadDes);
        keyedStream.connect(broadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Person, Person, Person>() {


                    @Override
                    public void open(Configuration parameters) throws Exception {

                    }

                    @Override
                    public void processElement(Person value, KeyedBroadcastProcessFunction<String, Person, Person, Person>.ReadOnlyContext ctx, Collector<Person> out) throws Exception {
                        out.collect(value);

                    }

                    @Override
                    public void processBroadcastElement(Person value, KeyedBroadcastProcessFunction<String, Person, Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                        BroadcastState<String, Person> broadcastState = ctx.getBroadcastState(broadDes);
                        broadcastState.put(value.getName(), value);
                        log.info("broadcast stream:{}",broadcastState.entries());
                    }
                })
                .keyBy(data -> data.getName())
                .process(new Test002(broadDes))
                .print();


        env.execute();
    }
}
