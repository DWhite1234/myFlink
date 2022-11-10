package com.spring.el;

import com.alibaba.fastjson.JSON;
import com.spring.ds.DS_001;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */
@Slf4j
public class test_001 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",999)
                .map(JSON::parseObject)
                .keyBy(data->data.getString("name"))
                .process(new DS_001());

        env.execute();
    }
}
