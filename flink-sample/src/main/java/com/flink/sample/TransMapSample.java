package com.flink.sample;

import com.flink.sample.common.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
        SingleOutputStreamOperator<String> mapStream = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        mapStream.print();
        env.execute();
    }
}
