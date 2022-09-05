package com.flink.sample;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransMapSample {
    public static void main(String[] args) throws Exception  {
        //simpleMaple();
        simpleMapleClass();
    }
    private static void simpleMaple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));
        stream.map(event ->event.user);
        stream.print();
        env.execute();
    }

    private static void simpleMapleClass() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));
        stream.map(new MapFunction<Event, Object>() {
            @Override
            public Object map(Event event) throws Exception {
                return event.user;
            }
        });
        stream.print();
        env.execute();
    }
}
