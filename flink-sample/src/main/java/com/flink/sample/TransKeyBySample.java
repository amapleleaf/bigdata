package com.flink.sample;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyBySample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./cart", 5000L),
                new Event("Bob", "./cart", 4000L),
                new Event("Alice", "./cart", 3000L),
                new Event("Bob", "./cart", 2000L));
        //streamSource.keyBy(e -> e.user).max("timestamp").print("max:");
        streamSource.keyBy(e -> e.user).max("timestamp").print("maxBy:");
        env.execute();
    }
}
