package com.zt.flink.ds.dataStream;

import com.zt.flink.ds.socket.WebSocketSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class KeyedSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",999)
                .map(data->data)
                .keyBy(data->data)
                .addSink(new WebSocketSink());

        env.execute();
    }
}
