import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhongtao/disk/workspace/数动/ck");
//        env.enableCheckpointing(3600000);
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("provide.transaction.metadata", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.211.55.101")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("mydb")
                .tableList("mydb.student")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                /*
                json格式:
                {"before":null,"after":{"s_id":111,"s_name":"zs","s_birth":19277,"s_sex":"男"},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"mydb","sequence":null,"table":"student","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1667996853502,"transaction":null}
                string格式:
                SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1667996934, file=, pos=0}} ConnectRecord{topic='mysql_binlog_source.mydb.student', kafkaPartition=null, key=Struct{s_id=111}, keySchema=Schema{mysql_binlog_source.mydb.student.Key:STRUCT}, value=Struct{after=Struct{s_id=111,s_name=zs,s_birth=19277,s_sex=男},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,db=mydb,table=student,server_id=0,file=,pos=0,row=0},op=r,ts_ms=1667996934647}, valueSchema=Schema{mysql_binlog_source.mydb.student.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
                 */
                .build();
                /*
                initial:
                    每次启动都会从头开始读取,只能读取到最后一次的修改
                earliest:
                    每次都会从最开始的修改开始读取,中间经历过的修改也会被读取到
                    要求:bin-log必须在创建数据库之前开启
                latest:
                    从最新的数据开始读取
                specificOffset:
                    从指定的offset开始读取
                timestamp:
                    从指定时间开始读取
                 */

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql cdc")
                .map(new MapFunction<String, String>() {
                    private Integer count = 0;
                    private Long sum = 0L;
                    @Override
                    public String map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        Long ts_ms = jsonObject.getLong("ts_ms");
                        long currentTimeMillis = System.currentTimeMillis();
                        sum += (currentTimeMillis - ts_ms);
                        count++;
//                        log.info("解析数据:{}条,总延迟:{},平均解析延迟:{},当前解析延迟:{}",count,sum,(sum / count * 1.0),currentTimeMillis - ts_ms);
                        return "解析数据:"+count+",总延迟:"+sum+",平均延迟:"+(sum / count * 1.0)+",当前解析延迟:"+(currentTimeMillis - ts_ms);
                    }
                }).print();

        env.execute();
    }
}
