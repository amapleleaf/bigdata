package com.flink.sample;

import com.flink.sample.common.ClickSource;
import com.flink.sample.common.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLTimeWindowSample {
    public static void main(String[] args) throws Exception {
        //smaple1();
        smaple2();
    }

    private static void smaple1() throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(evn);
        //DDL中直接定义时间属性
        String creatDDL = "CREATE TABLE events ( " +
                " ouser STRING, " +
                " url STRING," +
                " ts BIGINT," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                " WATERMARK FOR time_ltz as time_ltz - INTERVAL '1' SECOND" +
                " )WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample\\input\\clicks.txt', " +
                "'format'='csv' " +
                ")";
        TableResult tableResult = tableEnv.executeSql(creatDDL);
        Table table = tableEnv.sqlQuery("select * from events");
        table.printSchema();
        tableEnv.toChangelogStream(table).print("event");
        evn.execute();
    }

    private static void smaple2() throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        DataStreamSource<Event> streamSource = evn.addSource(new ClickSource());
        streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(evn);
        Table table = tableEnv.fromDataStream(streamSource, $("user"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        table.printSchema();
        tableEnv.toChangelogStream(table).print("event");
        evn.execute();
    }
}
