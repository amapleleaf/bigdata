package com.flink.sample;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Calendar;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLSample {
    public static void main(String[] args) throws Exception {
        //tabletoStream();
        //tabletoTable();
        streamToTable();
    }

    public static void tabletoStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /*EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);*/

        TableResult csvtable = tableEnv.executeSql("CREATE TABLE SSJS ( " +
                " ID STRING, " +
                " JSDM STRING, " +
                " JSFL_ID STRING, " +
                " JSMC STRING," +
                " JSMS STRING) " +
                "WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample/input/ss_js.csv', " +
                "'format'='csv' " +
                ")");
        Table table = tableEnv.sqlQuery("SELECT ID,JSDM,JSFL_ID,JSMC FROM  SSJS");
        tableEnv.toDataStream(table).print("ssjs");

        Table groupbytb = tableEnv.sqlQuery("SELECT JSFL_ID,count(1) FROM  SSJS GROUP BY JSFL_ID ");
        tableEnv.toChangelogStream(groupbytb).print("group by");

        env.execute();
    }

    public static void tabletoTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        TableResult csvtable = tableEnv.executeSql("CREATE TABLE SSJS ( " +
                " ID STRING, " +
                " JSDM STRING, " +
                " JSFL_ID STRING, " +
                " JSMC STRING," +
                " JSMS STRING " +
                ")WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample/input/ss_js.csv', " +
                "'format'='csv' " +
                ")");

        TableResult outputTb = tableEnv.executeSql("CREATE TABLE SSJSOUT ( " +
                " ID STRING, " +
                " JSDM STRING, " +
                " JSMC STRING" +
                " )WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample\\input', " +
                "'format'='csv' " +
                ")");

        TableResult outGroupTb = tableEnv.executeSql("CREATE TABLE SSJSGROUP ( " +
                " JSFL_ID STRING, " +
                " CNT BIGINT" +
                " )WITH( " +
                "'connector'='filesystem', " +
                "'path'='D:\\idealproject\\bigdata\\flink-sample\\input', " +
                "'format'='csv' " +
                ")");
        tableEnv.executeSql("INSERT INTO SSJSOUT SELECT ID,JSDM,JSMC FROM SSJS");

        //Table sqlQuery = tableEnv.sqlQuery("SELECT JSFL_ID,count(1) AS CNT FROM  SSJS GROUP BY JSFL_ID ");
        //sqlQuery.executeInsert("SSJSGROUP");
        //tableEnv.toChangelogStream(sqlQuery).print("group");
        //env.execute(); //executeSql() 方法已经执行了sql语句，不需要再使用execute()方法
    }

    public static void streamToTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(streamSource, $("user"), $("url"));
        table.printSchema();
        tableEnv.createTemporaryView("eventTable",table);
        Table grouptable = tableEnv.sqlQuery("select user,count(1) from eventTable group by user");
        tableEnv.toDataStream(table).print("table");
        tableEnv.toChangelogStream(grouptable).print("table");
        env.execute();
    }

    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random(); // 在指定的数据集中随机选取数据
            String[] users = {"Mary", "Alice", "Bob", "Cary"};
            String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                    "./prod?id=2"};
            while (running) {
                sourceContext.collect(new Event(
                        users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                // 隔 1 秒生成一个点击事件，方便观测
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
