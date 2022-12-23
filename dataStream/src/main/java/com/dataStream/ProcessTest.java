package com.dataStream;

import com.functions.Test004;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class ProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost",999)
                .map(new Test004())
                .print();

        env.execute();
    }
}
