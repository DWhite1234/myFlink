package com.zt.flink.ds.sink;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",999)
                .map(data-> JSON.parseObject(data, Person.class))
                .keyBy(data->data.getName())
                .addSink(new KeybySink()).setParallelism(2);

        env.execute();
    }
}
