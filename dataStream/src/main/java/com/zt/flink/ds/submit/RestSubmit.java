package com.zt.flink.ds.submit;

import jdk.nashorn.internal.runtime.regexp.JoniRegExp;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * @author zt
 */

public class RestSubmit {
    public static void main(String[] args) throws Exception {

        PackagedProgram packagedProgram = PackagedProgram.newBuilder()
                //主包
                .setJarFile(new File("/Users/zhongtao/disk/workspace/数动/mishap-flink/mishap-flink-single-reconciliation/target/mishap-flink-single-reconciliation-3.1.0-SNAPSHOT.jar"))
                //第三方包
                .setUserClassPaths(Arrays.asList(new URL("http://172.1.2.21:9000/base/flink/mishap-flink-algel-3.1.0-SNAPSHOT.jar")))
                //mainClass
                .setEntryPointClassName("com.myouchai.mishap.flink.RealTimeMishapRunner")
                //savepoint
                .setSavepointRestoreSettings(SavepointRestoreSettings.none())
                //参数
                .setArguments(args)
                .build();

        Configuration configuration = new Configuration();
        //jobmanager 地址
        configuration.setString(JobManagerOptions.ADDRESS, "172.1.2.202");
        //jobmanager rpc端口,默认6123
        configuration.setInteger(JobManagerOptions.PORT, 6123);
        //jobmanager 端口
        configuration.setInteger(RestOptions.PORT, 8088);
        
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                packagedProgram,
                configuration,
                1,
                false
        );
        RestClusterClient<StandaloneClusterId> restClusterClient = new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());
        CompletableFuture<JobID> completableFuture = restClusterClient.submitJob(jobGraph);
        JobID jobID = completableFuture.get();
        System.out.println(jobID);
    }
}
