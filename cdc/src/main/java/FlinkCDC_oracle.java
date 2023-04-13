import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class FlinkCDC_oracle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        //如下两项建议使用,否则数据变更延迟在几十秒到几分钟
        //redo_log_catalog:可以跟踪DDL,适合数据变更频繁的
        //online_catalog:不能跟踪DDL,适合数据变更不频繁的
        properties.setProperty("log.mining.strategy", "online_catalog");
        //poll interval,default 1000
        properties.setProperty("poll.interval.ms", "100");
        //sleep default interval,default value 1000
        properties.setProperty("log.mining.sleep.time.default.ms", "100");
        //sleep max interval,default value 3000
        properties.setProperty("log.mining.sleep.time.max.ms", "100");
        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("172.1.2.41")
                .port(1521)
                .database("helowin")
                .schemaList("SDC_TEST")
                .tableList("SDC_TEST.student")
                .username("SDC_TEST")
                .password("666666")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
//        env.addSource(sourceFunction)
//                .map(JSON::parseObject)
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                                    @Override
//                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                                        return (Long) element.getLong("ts_ms");
//                                    }
//                                }))
//                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
//                .trigger(CountTrigger.of(1))
//                .process(new ProcessAllWindowFunction<JSONObject, Integer, TimeWindow>() {
//                    @Override
//                    public void process(ProcessAllWindowFunction<JSONObject, Integer, TimeWindow>.Context context, Iterable<JSONObject> elements, Collector<Integer> out) throws Exception {
//                        int sum = 0;
//                        for (JSONObject element : elements) {
//                            sum += 1;
//                        }
//                        out.collect(sum);
//                    }
//                })
//                .print();


        env.addSource(sourceFunction)
                .map(new MapFunction<String, String>() {
                    private Integer count = 0;
                    private Long sum = 0L;

                    @Override
                    public String map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        System.out.println("jsonObject = " + jsonObject);
                        JSONObject after = JSON.parseObject(jsonObject.get("after").toString());
                        Long ts_ms = after.getLong("S_BIRTH");
                        long currentTimeMillis = System.currentTimeMillis();
                        sum += (currentTimeMillis - ts_ms);
                        count++;
                        return "解析数据:" + count + ",总延迟:" + sum + ",当前解析延迟:" + (currentTimeMillis - ts_ms) + ",平均延迟:" + (sum / count * 1.0);
                    }
                }).print();


        env.execute();
    }
}
