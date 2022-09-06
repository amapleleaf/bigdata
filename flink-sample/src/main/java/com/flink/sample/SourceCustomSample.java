package com.flink.sample;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

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
        DataStreamSource<Event> customSource = env.addSource(new ClickSource());
        customSource.print();
        env.execute();
    }

    public static class ClickSource  implements SourceFunction<Event> {
        private boolean running=true;
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
            running=false;
        }
    }


    public static class ParalleClickSource  implements ParallelSourceFunction<Event> {
        private boolean running=true;
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
            running=false;
        }
    }
}
