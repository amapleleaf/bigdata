package com.flink.sample;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class TopNSample {
    public static void main(String[] args) throws Exception {
        topN();
    }
    private static void topN() throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(evn);
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
        //Table table = tableEnv.sqlQuery("SELECT ouser,COUNT(url) AS cnt FROM events GROUP BY ouser ");

        //所有点击量排行
        Table topN = tableEnv.sqlQuery("SELECT ouser,cnt,row_num" +
                " FROM (" +
                "   SELECT *,ROW_NUMBER() OVER(" +
                "           ORDER BY cnt DESC" +
                "       ) AS row_num FROM (SELECT ouser,COUNT(url) AS cnt FROM events GROUP BY ouser )" +
                " ) WHERE row_num<=3");
        //tableEnv.toChangelogStream(topN).print("top3");

        //统计一段时间内点击量排行
        String subQuery="SELECT ouser,count(1) as cnt,window_start,window_end" +
                " FROM TABLE( " +
                "   TUMBLE(TABLE events,DESCRIPTOR(time_ltz),INTERVAL '10' SECOND)" +
                ")  GROUP BY ouser,window_end,window_start";
        Table subTable = tableEnv.sqlQuery(subQuery);
        subTable.printSchema();
        //tableEnv.toChangelogStream(subTable).print("subTable");

        Table widowTopN = tableEnv.sqlQuery("SELECT ouser,cnt,row_num" +
                " FROM (" +
                "   SELECT *,ROW_NUMBER() OVER(" +
                "           PARTITION BY window_start,window_end" +
                "           ORDER BY cnt DESC" +
                "       ) AS row_num FROM ("+subQuery+" )" +
                " ) WHERE row_num<=2");

        tableEnv.toChangelogStream(widowTopN).print("widowTopN");

        evn.execute();
    }
}
