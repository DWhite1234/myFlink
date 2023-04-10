package com.zt.flink.ds.dataStream;

import com.alibaba.fastjson.JSON;
import com.common.beans.Person;
import com.zt.flink.java.utils.KafkaUtils;
import com.zt.flink.java.utils.RandomUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.regex.Pattern;


/**
 * @author zt
 */
@Slf4j
public class MainTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("localhost",999)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        log.info("当前随机数为:{}",RandomUtils.getRange(0, 10));
                        System.out.println("当前随机数为:"+RandomUtils.getRange(0, 10));
                        return value;
                    }
                })
                .print();
        env.execute();
    }
}
