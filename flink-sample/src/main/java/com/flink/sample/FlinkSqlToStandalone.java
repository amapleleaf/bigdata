package com.flink.sample;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint.ExecutionMode;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Optional;

public class FlinkSqlToStandalone {
    public static void main(String[] args) {
        String flinkHost="192.168.226.110";
        int flinkRestPort=8081;
        Configuration configuration = new Configuration();
        RemoteStreamEnvironment remoteStreamEnvironment =  new RemoteStreamEnvironment(flinkHost,flinkRestPort,
                configuration);
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(remoteStreamEnvironment);
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
        stEnv.executeSql(sourceSql);
        stEnv.executeSql(sinkSql);
        TableResult tableResult = stEnv.executeSql(insertSql);
        Optional<JobClient> jobClient = tableResult.getJobClient();
        JobClient jobClient1 = jobClient.get();
        JobID jobID = jobClient1.getJobID();
        System.out.println(jobID);
    }
}
