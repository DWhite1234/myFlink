package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class TestConnect {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 999);
        SingleOutputStreamOperator<ProcessTest> ds2 = env.socketTextStream("localhost", 888)
                .map(data -> JSON.parseObject(data, ProcessTest.class));
        ds1.connect(ds2);
    }
}
