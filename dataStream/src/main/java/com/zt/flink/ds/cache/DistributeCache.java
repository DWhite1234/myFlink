package com.zt.flink.ds.cache;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.List;

/**
 * @author zt
 */
@Slf4j
public class DistributeCache {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("/Users/zhongtao/disk/workspace/mine/myFlink/dataStream/src/main/resources","cache.txt");
        env.setParallelism(1);

        env.socketTextStream("localhost", 999)
                .map(new RichMapFunction<String, String>() {

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        File file = getRuntimeContext().getDistributedCache().getFile("cache.txt");
                        File absoluteFile = file.getAbsoluteFile();
                        List<String> lines = FileUtils.readLines(absoluteFile);
                        log.info(lines.toString());
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return null;
                    }
                })
                .print();

        env.execute();
    }
}
