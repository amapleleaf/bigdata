package com.flink.sample;

import com.alibaba.fastjson.JSONObject;
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

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlinkSqlClientDemo {
    public FlinkSqlClientDemo() {
    }

    public static void main(String[] args) {
        String jarFilePath = "C:\\Users\\cestc\\Desktop\\flink\\flink-sql-submit-sdk.jar";
        RestClusterClient client = null;

        try {
            Configuration configuration = new Configuration();
            configuration.setString(JobManagerOptions.ADDRESS, "192.168.226.110");
            configuration.setInteger(JobManagerOptions.PORT, 6123);
            configuration.setInteger(RestOptions.PORT, 8081);
            configuration.setString(ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, ExecutionMode.DETACHED.toString());
            client = new RestClusterClient(configuration, StandaloneClusterId.getInstance());
            int parallelism = 1;
            File jarFile = new File(jarFilePath);

            String[] pare = new String[]{jsonStr()};
            SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
            long start = System.currentTimeMillis();
            PackagedProgram program = PackagedProgram.newBuilder().setConfiguration(configuration).setEntryPointClassName("com.cestc.bigdataclient.sqlsubmit.SqlSubmit").setJarFile(jarFile).setArguments(pare).setSavepointRestoreSettings(savepointRestoreSettings).build();
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, configuration, parallelism, true);
            CompletableFuture<JobID> result = client.submitJob(jobGraph);
            CompletableFuture<JobResult> jobResult = client.requestJobResult((JobID)result.get());
            JobResult hhh = (JobResult)jobResult.get();
            Map<String, SerializedValue<OptionalFailure<Object>>> accumulatorResults = hhh.getAccumulatorResults();
            JobID jobId = (JobID)result.get();
            System.out.println("提交完成,耗时:"+(System.currentTimeMillis()-start));
            System.out.println("jobId:" + jobId.toString());
        } catch (Exception var15) {
            var15.printStackTrace();
        }

    }

    private static String jsonStr() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jobId","flink-sql-test");
        jsonObject.put("taskMode","1");
        //"redisVO":{"password":"java_redis_dev@password","database":"0","port":"6379","ip":"10.101.232.27"}
        JSONObject redisObject = new JSONObject();
        redisObject.put("password","java_redis_dev@password");
        redisObject.put("database","0");
        redisObject.put("ip","10.255.158.176");
        redisObject.put("port","9531");

        String sourceSql = "CREATE TABLE KafkaTable ( "
                +"username STRING,"
                +"requrl STRING,"
                +"reqtime BIGINT,"
                +"dealtime TIMESTAMP(3) METADATA FROM 'timestamp'"
                +") WITH ("
                +"'connector' = 'kafka',"
                +"'topic' = 'test_topic',"
                +"'properties.bootstrap.servers' = '192.168.226.110:9092',"
                +"'properties.group.id' = 'flink-kafka',"
                +"'scan.startup.mode' = 'latest-offset',"
                +"'format' = 'json'"
                +")";
        String sinkSql = "CREATE TABLE UserEvent(" +
                "username STRING," +
                "requrl STRING," +
                "reqtime BIGINT," +
                "dealtime TIMESTAMP(3)"+
                ") WITH(" +
                "'connector.type'='jdbc'," +
                "'connector.url'='jdbc:mysql://192.168.226.110:9511/flink_test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai'," +
                "'connector.table'='userevent'," +
                "'connector.username'='root'," +
                "'connector.password'='mysql123%^PASS'," +
                "'connector.write.flush.max-rows'='1'" +
                ")";
        String insertSql="insert into UserEvent(username,requrl,reqtime,dealtime) select username,requrl,reqtime,dealtime from KafkaTable";
        //String insertSql="select * from KafkaTable";
        jsonObject.put("sql",sourceSql+";"+sinkSql+";"+insertSql);
        String jsonStr =  jsonObject.toJSONString();
        System.out.println(jsonStr);
        try {
            return URLEncoder.encode(jsonStr, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }
    }
}
