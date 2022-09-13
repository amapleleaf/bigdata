package com.flink.sample;

import com.flink.sample.common.ClickSource;
import com.flink.sample.common.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class SQLUDFSample {
    public static void main(String[] args) throws Exception {
        //udf1();//标量函数
        //udf2(); //表函数
          udf3();//聚合函数
        //udf4();//表聚合函数
    }

    //标量函数
    public static void udf1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<Event> waterStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table mytable = tableEnv.fromDataStream(waterStream, $("user").as("user_name"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        tableEnv.createTemporaryView("mytable", mytable);
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFunction.class);
        Table hashTable = tableEnv.sqlQuery("select user_name,MyHash(user_name) from mytable");
        mytable.printSchema();
        tableEnv.toChangelogStream(hashTable).print("hashTable");
        env.execute();
    }

    //表函数
    public static void udf2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<Event> waterStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table mytable = tableEnv.fromDataStream(waterStream, $("user").as("user_name"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        tableEnv.createTemporaryView("mytable", mytable);
        tableEnv.createTemporarySystemFunction("MySplit", SplitFunction.class);
        Table hashTable = tableEnv.sqlQuery("select user_name,url,word,length from mytable ,LATERAL TABLE( MySplit(url) ) AS T(word,length)");
        mytable.printSchema();
        tableEnv.toChangelogStream(hashTable).print("mytable");
        env.execute();
    }

    //聚合函数
    public static void udf3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<Event> waterStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table mytable = tableEnv.fromDataStream(waterStream, $("user").as("user_name"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        tableEnv.createTemporaryView("mytable", mytable);
        tableEnv.createTemporarySystemFunction("WeightedAvg", WeightedAvg.class);
        Table hashTable = tableEnv.sqlQuery("select user_name,WeightedAvg(ts,1) as w_avg from mytable group by user_name");
        mytable.printSchema();
        tableEnv.toChangelogStream(hashTable).print("mytable");
        env.execute();
    }

    //表聚合函数
    public static void udf4() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        SingleOutputStreamOperator<Event> waterStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table mytable = tableEnv.fromDataStream(waterStream, $("user").as("user_name"), $("url"), $("timestamp").as("ts"), $("et").proctime());
        tableEnv.createTemporaryView("mytable", mytable);
        tableEnv.createTemporarySystemFunction("Top2", Top2.class);
        Table hashTable = tableEnv.sqlQuery("select user_name,WeightedAvg(ts,1) as w_avg from mytable group by user_name");
        mytable.printSchema();
        tableEnv.toChangelogStream(hashTable).print("mytable");
        env.execute();
    }

    // 自定义表聚合函数，查询一组数中最大的两个，返回值为(数值，排名)的二元组
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accumulator> {
        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;    // 为方便比较，初始值给最小值
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        // 每来一个数据调用一次，判断是否更新累加器
        public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 输出(数值，排名)的二元组，输出两行数据
        public void emitValue(Top2Accumulator acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

    // 聚合累加器的类型定义，包含最大的第一和第二两个数据
    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }

    public static class WeightedAvgAccumulator {
        public long sum = 0;    // 加权和
        public int count = 0;    // 数据个数
    }

    public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();    // 创建累加器
        }

        @Override
        public Long getValue(WeightedAvgAccumulator acc) {
            if (acc.count == 0) {
                return null;    // 防止除数为 0
            } else {
                return acc.sum / acc.count;    // 计算平均值并返回
            }
        }

        // 累加计算方法，每来一行数据都会调用
        public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }
    }

    //@FunctionHint(output = @DataTypeHint("Tuple2<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Tuple2<String, Integer>> {
        public void eval(String str) {
            for (String s : str.split("\\?")) {       // 使用 collect()方法发送一行数据
                collect(Tuple2.of(s, s.length()));
            }
        }
    }

    public static class MyHashFunction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
