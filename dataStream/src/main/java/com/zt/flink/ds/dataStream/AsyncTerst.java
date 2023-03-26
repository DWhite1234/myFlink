package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.zt.flink.ds.io.AsyncFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @author zt
 */
@Slf4j
public class AsyncTerst {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 999);
        KeyedStream<Person, String> keyedStream = source.map(data -> JSON.parseObject(data, Person.class))
                .keyBy(data -> data.getName());
        AsyncDataStream.unorderedWait(keyedStream, new AsyncFunction(), 1000, TimeUnit.SECONDS)
                .print();


        env.execute();
    }
}
