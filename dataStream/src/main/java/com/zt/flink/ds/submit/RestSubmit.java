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
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * @author zt
 */

public class RestSubmit {
    public static void main(String[] args) throws Exception {
        PackagedProgram packagedProgram = PackagedProgram.newBuilder()
                .setJarFile(new File("/Users/zhongtao/disk/workspace/mine/myFlink/dataStream/target/flink-main-test.jar"))
                .setEntryPointClassName("com.zt.flink.ds.dataStream.MainTest")
                .setSavepointRestoreSettings(SavepointRestoreSettings.none())
                .setArguments(args)
                .build();



        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, "172.1.2.202");
        configuration.setInteger(JobManagerOptions.PORT, 6123);
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
