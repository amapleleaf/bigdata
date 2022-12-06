package com.flink.sample;

import org.apache.commons.io.FileUtils;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

public class SubmitStandaloneTaskSample {
    public static void main(String[] args) throws Exception {
        //构建配置信息
        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, "192.168.226.110");
        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        configuration.setInteger(JobManagerOptions.PORT,6123);
        configuration.setInteger(RestOptions.PORT, 8081);


        RestClusterClient<StandaloneClusterId> client =
                new RestClusterClient<>(configuration, StandaloneClusterId.getInstance());



       // 续跑从配置中读取 savepoint；提交则设置 savepoint 为空
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

/*
        PackagedProgram.newBuilder()
                .setConfiguration(configuration)
                .setEntryPointClassName(FLINK_SUBMIT_JAR_MAIN_CLASS)
                .setJarFile(FileUtils.getFile(flinkTaskRunVO.getTaskJarPath()))
                .setArguments(submitStr)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();

        JobGraph jobGraph= PackagedProgramUtils.createJobGraph(program,configuration,1,false);
        */

    }
}
