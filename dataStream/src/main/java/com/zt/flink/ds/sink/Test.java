package com.zt.flink.ds.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author zt
 */

public class Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        new FlinkKafkaProducer<String>("", "", new SimpleStringSchema());
        DataStreamSink<String> streamSink = env.fromElements("1")
                .addSink(new TwoStageKafkaSink(null,null));
    }
}
