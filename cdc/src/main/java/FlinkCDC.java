import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zt.flink.java.utils.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("provide.transaction.metadata", "true");
        /*
        During the incremental snapshot reading, the MySQL CDC Source firstly splits snapshot chunks (splits) by primary key of table,
        and then MySQL CDC Source assigns the chunks to multiple readers to read the data of snapshot chunk.

        cdc是根据主键进行分片的,所以表应该设置主键

        The mysql-cdc connector offers high availability of MySQL high available cluster by using the GTID information. To obtain the high availability, the MySQL cluster need enable the GTID mode,
        the GTID mode in your mysql config file should contain following settings:
        # gtid_mode = on
enfo    # rce_gtid_consistency = on

        If the monitored MySQL server address contains slave instance, you need set following settings to the MySQL conf file. The setting log-slave-updates = 1 enables the slave instance to also write the data that synchronized from master to its binlog,
        this makes sure that the mysql-cdc connector can consume entire data from the slave instance.
        # gtid_mode = on
        # enforce_gtid_consistency = on
        #* log-slave-updates = 1 开启此项会允许主库写binlog到从库,从而确保消费从库可以获取到完整的数据
         */
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.211.55.101")
                /*
                If the table updates infrequently, the binlog file or GTID set may have been cleaned in its last committed binlog position.
                The CDC job may restart fails in this case. So the heartbeat event will help update binlog position.
                By default heartbeat event is enabled in MySQL CDC source and the interval is set to 30 seconds

                如果数据更新不频繁,binlog可能被清除,导致job重启失败.开启心跳可以更新binlog position,因此建议开启
                心跳间隔,默认30s,建议开启
                 */
                .heartbeatInterval(Duration.ofSeconds(30))
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("mydb")
                .tableList("mydb.student")
                .startupOptions(StartupOptions.latest())
                /*
                If you would like the source run in parallel, each parallel reader should have an unique server id,
                so the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism

                如果想开启多并行度读取,一定要给每个并行度设置不同的serverId
                 */
                .serverId(RandomUtils.getRange(5,10) +"-"+System.currentTimeMillis())
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
                .print();

        env.execute();
    }
}
