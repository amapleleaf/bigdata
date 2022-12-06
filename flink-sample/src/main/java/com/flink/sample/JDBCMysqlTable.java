package com.flink.sample;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JDBCMysqlTable {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .useBlinkPlanner() // blink
                    .build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
            tableEnv.executeSql("CREATE TABLE  EventTable (\n" +
                    "user_name STRING,\n" +
                    "req_url STRING,\n" +
                    "req_time BIGINT\n" +
                    ") WITH (\n" +
                    "'connector.type' = 'jdbc',\n" +
                    //"'connector.driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                    "'connector.url' = 'jdbc:mysql://192.168.226.110:9511/flink_test?useSSL=false',\n" +
                    "'connector.table' = 'flink_event',\n" +
                    "'connector.username' = 'root',\n" +
                    "'connector.password' = 'mysql123%^PASS',\n" +
                    "'connector.write.flush.max-rows' = '1'\n" +
                 /*   "'connector.lookup.cache.max-rows' = '1',\n"+
                    "'connector.lookup.cache.ttl' = '1000'\n"+*/
                   // "'connector.write.flush.interval' = '100'\n" +
                    ")");
            Table mysql_user = tableEnv.from("EventTable");
            mysql_user.printSchema();
            tableEnv.executeSql("select * from EventTable").print();
           /* Table table = tableEnv.sqlQuery("select * from EventTable");
            tableEnv.toDataStream(table).print("table");
            env.execute();*/
            //TableResult tableResult = tableEnv.executeSql("insert into EventTable(user_name,req_url,req_time) values('Alice','./prod?id=3',106000)");
            //tableResult.getJobClient().get().getJobExecutionResult().get();
            System.out.println("-------------end--------------------------");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
