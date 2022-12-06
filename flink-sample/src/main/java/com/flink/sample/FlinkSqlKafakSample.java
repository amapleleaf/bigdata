package com.flink.sample;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlKafakSample {
    public static void main(String[] args) throws Exception {
        readFromKafka();
    }

    public static void readFromKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner() //使用blink的计划器
                .inStreamingMode() //使用流模型
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sourceSql = "CREATE TABLE KafkaTable ( "
                +"userName STRING,"
                +"reqUrl STRING,"
                +"reqTime BIGINT,"
                +"dealTime TIMESTAMP(3) METADATA FROM 'timestamp'"
                +") WITH ("
                +"'connector' = 'kafka',"
                +"'topic' = 'test_topic',"
                +"'properties.bootstrap.servers' = '192.168.226.110:9092',"
                +"'properties.group.id' = 'flink-kafka',"
                +"'scan.startup.mode' = 'latest-offset',"
                +"'format' = 'json'"
                +")";
        TableResult kafkaTable = tableEnv.executeSql(sourceSql);

        Table table = tableEnv.sqlQuery("SELECT * FROM KafkaTable");
        tableEnv.toDataStream(table).print("kafka data");
        env.execute();
    }
}
