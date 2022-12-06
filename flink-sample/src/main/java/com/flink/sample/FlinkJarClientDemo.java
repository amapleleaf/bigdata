package com.flink.sample;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint.ExecutionMode;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

public class FlinkJarClientDemo {
    public FlinkJarClientDemo() {
    }

    public static void main(String[] args) {
            String jarFilePath = "C:\\Users\\cestc\\Desktop\\flink\\flink-sql-submit-client-1.0-SNAPSHOT.jar";
            RestClusterClient<StandaloneClusterId> client = null;
            try {
                // 集群信息
                Configuration configuration = new Configuration();
                configuration.setString(JobManagerOptions.ADDRESS, "192.168.226.110");
                configuration.setInteger(JobManagerOptions.PORT, 6123);
                configuration.setInteger(RestOptions.PORT, 8081);
                configuration.setString(ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, ExecutionMode.DETACHED.toString());
                client = new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
                int parallelism = 1;
                File jarFile=new File(jarFilePath);
                SavepointRestoreSettings savepointRestoreSettings=SavepointRestoreSettings.none();
                PackagedProgram program = PackagedProgram.newBuilder()
                        .setConfiguration(configuration)
                        .setEntryPointClassName("com.cestc.bigdataclient.sqlsubmit.FlinkKafakSample")
                        .setJarFile(jarFile)
                        .setSavepointRestoreSettings(savepointRestoreSettings).build();
                long start = System.currentTimeMillis();
                JobGraph jobGraph=PackagedProgramUtils.createJobGraph(program,configuration,parallelism,false);
                CompletableFuture<JobID> result = client.submitJob(jobGraph);
                JobID jobId=  result.get();
                System.out.println("提交完成,耗时:"+(System.currentTimeMillis()-start));
                System.out.println("jobId:"+ jobId.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}
