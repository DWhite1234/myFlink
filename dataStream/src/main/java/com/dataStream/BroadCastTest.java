package com.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.esotericsoftware.minlog.Log;
import com.spring.utils.SpelUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zt
 */
@Slf4j
public class BroadCastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<String> map = env.socketTextStream("localhost", 999);


        SingleOutputStreamOperator<Person> broad = env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, Person.class));
        MapStateDescriptor<String, Person> broadDes = new MapStateDescriptor<>("broad", String.class, Person.class);
        BroadcastStream<Person> broadcastStream = broad.broadcast(broadDes);
        map.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, Person, Person>() {
                    @Override
                    public void processElement(String value, BroadcastProcessFunction<String, Person, Person>.ReadOnlyContext ctx, Collector<Person> out) throws Exception {
                        log.info("当前线程:{},当年线程的上下文对象:{}", Thread.currentThread().getId(), SpelUtil.getContext().lookupVariable("ww"));
                    }

                    @Override
                    public void processBroadcastElement(Person value, BroadcastProcessFunction<String, Person, Person>.Context ctx, Collector<Person> out) throws Exception {
                        log.info("当前线程:{},广播设置上下文", Thread.currentThread().getId());
                        SpelUtil.setContextVariable(value.getName(), value);
                    }
                }).print();


        env.execute();
    }
}
