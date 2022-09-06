package com.flink.sample;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class WordCountSample {

    public static void main(String[] args) throws Exception {
        //test1();
        test2();
    }

    public static void test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\idealproject\\bigdata\\flink-sample\\input\\words.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        KeyedStream<Tuple2<String, Long>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2StringKeyedStream.sum(1);
        sum.print();
        env.execute();
    }

    public static void test2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\idealproject\\bigdata\\flink-sample\\input\\words.txt");
        stringDataStreamSource.flatMap((String line,Collector<String> out)->{
            Arrays.stream(line.split(" ")).forEach(out::collect);
        }).returns(new TypeHint<String>() {
        }).map(f -> Tuple2.of(f,1)).returns(new TypeHint<Tuple2<String, Integer>>() {
        }).keyBy(t ->t.f0).sum(1).print();
        env.execute();
    }

}
