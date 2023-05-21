package com.flink.sample;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlMysqlToMysqlSample {
    public static void main(String[] args) throws Exception {
        readFromKafka();
    }

    public static void readFromKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sourceSql = "CREATE TABLE UserTest(" +
                "username STRING," +
                "requrl STRING," +
                "reqtime BIGINT," +
                "dealtime TIMESTAMP(3)"+
                ") WITH(" +
                "'connector.type'='jdbc'," +
                "'connector.url'='jdbc:mysql://192.168.226.110:9511/flink_test?characterEncoding=UTF-8&useUnicode=true&useSSL=false&tinyInt1isBit=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai'," +
                "'connector.table'='usertest'," +
                "'connector.username'='root'," +
                "'connector.password'='mysql123%^PASS'," +
                "'connector.write.flush.max-rows'='1'" +
                ")";
        TableResult kafkaTable = tableEnv.executeSql(sourceSql);

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
        tableEnv.executeSql(sinkSql);
       /* Table table = tableEnv.sqlQuery("SELECT * FROM KafkaTable");
        tableEnv.toDataStream(table).print("kafka data");
        env.execute("flink_running");*/
        String insertSql="insert into UserEvent(username,requrl,reqtime,dealtime) select username,requrl,reqtime,dealtime from UserTest";
        TableResult tableResult = tableEnv.executeSql(insertSql);
        JobExecutionResult jobExecutionResult = tableResult.getJobClient().get().getJobExecutionResult().get();
        jobExecutionResult.getJobID();
    }
}
