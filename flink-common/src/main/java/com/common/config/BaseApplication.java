package com.common.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * 所有flink 启动类的父类
 *
 * @author zt
 */
@Slf4j
public class BaseApplication {
    protected static StreamExecutionEnvironment env;
    protected static ParameterTool parameterTool;

    protected static void buildMergeParameterTool(String[] args) {
        try (InputStream is = BaseApplication.class.getClassLoader().getResourceAsStream("flink.properties")) {
            parameterTool = ParameterTool.fromPropertiesFile(is);
        } catch (IOException ioException) {
            log.error("加载flink.properties异常", ioException);
        }
        ParameterTool argsParameterTool = ParameterTool.fromArgs(args);
        if (parameterTool == null) {
            parameterTool = argsParameterTool;
        } else {
            parameterTool = parameterTool.mergeWith(argsParameterTool);
        }
    }

    protected static void configureStreamExecutionEnvironment(String[] args) {
        buildMergeParameterTool(args);
        Configuration config = new Configuration();
        if (parameterTool.getBoolean("enableWeiUi")) {
            config.setInteger("rest.port",9999);
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        } else if (parameterTool.getBoolean("enableLocalCheckpoint")) {
            env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        }
        //配置对象设置到Global
        env.getConfig().setGlobalJobParameters(parameterTool);
        //重启策略
        env.setRestartStrategy(
                RestartStrategies.
                        fixedDelayRestart(3,
                                Time.of(5, TimeUnit.SECONDS)));
        //是否开启checkpoint
        if (parameterTool.getBoolean("enableCheckpoint")) {
            enableCheckpoint(parameterTool);
        }
    }


    protected static void enableCheckpoint(ParameterTool config) {
        //多久执行一次checkpoint操作, 默认 500ms, 当前5分钟
        env.enableCheckpointing(config.getLong("checkpointInterval"));
        //两次ck之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getLong("checkpointMinPauseBetween",60000L));
        //设置checkpoint超时时间, 如果checkpoint在60s内尚未完成, 则直接丢弃, 算失败
        env.getCheckpointConfig().setCheckpointTimeout(config.getLong("checkpointTimeout",60000L));
        //设置同一时间有多少个checkpoint 可以同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(config.getInt("checkpointMaxConcurrent",1));
        //取消作业时 不删除checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        String checkpointStateStoreType = config.get("checkpointStateStoreType");
        switch (checkpointStateStoreType) {
            case "memory":
                env.setStateBackend(new HashMapStateBackend());
                break;
            case "rocksdb":
                env.setStateBackend(new EmbeddedRocksDBStateBackend());
                break;
            default:
                log.warn("不支持的'{}'类型的StateBackend", checkpointStateStoreType);

        }
        env.getCheckpointConfig().setCheckpointStorage(config.get("checkpointPath"));
    }
}
