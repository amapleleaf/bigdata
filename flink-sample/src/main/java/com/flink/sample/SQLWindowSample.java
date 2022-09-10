package com.flink.sample;

import com.flink.sample.common.ClickSource;
import com.flink.sample.common.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

import static org.apache.flink.table.api.Expressions.$;

public class SQLWindowSample {
    public static void main(String[] args) throws Exception {
        //smaple1();
       smaple2();
    }

    private static void smaple1() throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(evn);
        ZoneId zoneId= tableEnv.getConfig().getLocalTimeZone();
        //DDL中直接定义时间属性
        String creatDDL = "CREATE TABLE events ( " +
                " ouser STRING, " +
                " url STRING," +
                " ts BIGINT ," +
                " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                " WATERMARK FOR time_ltz as time_ltz - INTERVAL '1' SECOND" +
                " )WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample\\input\\clicks.txt', " +
                "'format'='csv' " +
                ")";
        tableEnv.executeSql(creatDDL);
        Table table = tableEnv.sqlQuery("select ouser,url,DATE_FORMAT(time_ltz ,'yyyy-MM-dd HH:mm:ss'),time_ltz from events");
        table.printSchema();
        tableEnv.toChangelogStream(table).print("event");
        evn.execute();
    }

    private static void smaple2() throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        DataStreamSource<Event> streamSource = evn.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> streamOperator = streamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(evn);
        //ZoneId zoneId= tableEnv.getConfig().getLocalTimeZone();
        //System.out.println(zoneId.getId());
        //tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        Table table = tableEnv.fromDataStream(streamOperator, $("user").as("user_name"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        table.printSchema();
        tableEnv.createTemporaryView("clicktable",table);
        tableEnv.toChangelogStream(table).print("event");
        //滚动窗口
        Table tumbleTable = tableEnv.sqlQuery("SELECT user_name,count(1) as cnt,window_end as end_time" +
                " FROM TABLE( " +
                "   TUMBLE(TABLE clicktable,DESCRIPTOR(et),INTERVAL '10' SECOND)" +
                ")  GROUP BY user_name,window_end,window_start");
        //tableEnv.toChangelogStream(tumbleTable).print("group window");

        //滑动窗口
        Table hopTable = tableEnv.sqlQuery("SELECT user_name,count(1) as cnt,window_end as end_time" +
                " FROM TABLE( " +
                "   HOP(TABLE clicktable,DESCRIPTOR(et),INTERVAL '5' SECOND,INTERVAL '10' SECOND)" +
                ")  GROUP BY user_name,window_end,window_start");

        //累计窗口
        Table cumulateTable = tableEnv.sqlQuery("SELECT user_name,count(1) as cnt,window_end as end_time" +
                " FROM TABLE( " +
                "   CUMULATE(TABLE clicktable,DESCRIPTOR(et),INTERVAL '10' SECOND,INTERVAL '60' SECOND)" +
                ")  GROUP BY user_name,window_end,window_start");


        //开窗函数
        Table openTable = tableEnv.sqlQuery("SELECT user_name,COUNT(url) OVER wd AS cnt,MAX(url) OVER wd as max_url" +
                " FROM clicktable" +
                " WINDOW wd AS (PARTITION BY user_name" +
                " ORDER BY et" +
                " ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
        //tableEnv.toChangelogStream(tumbleTable).print("tumbleTable window");
       //tableEnv.toChangelogStream(hopTable).print("hopTable window");
        //tableEnv.toChangelogStream(cumulateTable).print("cumlateTable window");
        tableEnv.toChangelogStream(openTable).print("openTable");

        evn.execute();

    }
}
