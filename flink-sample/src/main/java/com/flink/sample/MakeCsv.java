package com.flink.sample;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.sample.common.Event;
import org.apache.flink.table.runtime.util.JsonUtils;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

public class MakeCsv {
    public static void main(String[] args) throws InterruptedException {
        sendTokafka();
    }
    public static void makeCsv(){
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                "./prod?id=2"};
        StringBuffer line = new StringBuffer();
        for (int i = 0; i <30; i++) {
            line.append(users[random.nextInt(users.length)])
                    .append(",")
                    .append(urls[random.nextInt(urls.length)])
                    .append(",").append(Calendar.getInstance().getTimeInMillis())
                    .append("\r\n");
            // 隔 1 秒生成一个点击事件，方便观测
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.printf(line.toString());

        TimeZone timeZone = TimeZone.getDefault();
        System.out.println(timeZone.getDisplayName());
        System.out.println(timeZone.getID());
        System.out.println(timeZone.getID());
    }
    static void sendTokafka(){
        Random random = new Random();
        String[] users = {"sunwukong", "tangseng", "saheshang", "zhubajie","bailongma"};
        String[] urls = {"/home", "/cart", "/fav", "/buy","/order","/productlist","/productdetail"};
        StringBuffer line = new StringBuffer();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.226.110:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        JSONObject jsonObject;
        for (int i = 0; i <10; i++) {
            jsonObject = new JSONObject();
            jsonObject.put("username",users[random.nextInt(users.length)]);
            jsonObject.put("requrl",urls[random.nextInt(urls.length)]);
            jsonObject.put("reqtime",Calendar.getInstance().getTimeInMillis());
            kafkaProducer.send(new ProducerRecord<String, String>("test_topic", jsonObject.toJSONString()));
            System.out.println(jsonObject.toJSONString());
            try {
                Thread.sleep(random.nextInt(100)+400);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        kafkaProducer.close();
    }
}
