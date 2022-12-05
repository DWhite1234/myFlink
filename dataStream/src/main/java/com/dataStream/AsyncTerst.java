package com.dataStream;

import com.functions.AsyncFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

        AsyncDataStream.unorderedWait(source, new AsyncFunction(), 1000, TimeUnit.SECONDS)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        log.info("map:{}",value);
                        return value;
                    }
                })
                .print();


        env.execute();
    }
}
