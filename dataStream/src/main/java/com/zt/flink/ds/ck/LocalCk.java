package com.zt.flink.ds.ck;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zt
 */

public class LocalCk {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.setString("");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhongtao/disk/develop/ck/20221215");
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setParallelism(1);

        env.execute();
    }
}
