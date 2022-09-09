package com.flink.sample;

import com.flink.sample.common.ClickSource;
import com.flink.sample.common.Event;
import com.flink.sample.common.ParalleClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SourceCustomSample {
    public static void main(String[] args) throws Exception {
        //custsom1();
        paralleSource();
    }
    public  static void custsom1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> customSource = env.addSource(new ClickSource());
        customSource.print();
        env.execute();
    }

    public  static void paralleSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> customSource = env.addSource(new ParalleClickSource());
        customSource.print();
        env.execute();
    }
}
