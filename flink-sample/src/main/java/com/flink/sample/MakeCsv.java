package com.flink.sample;

import com.flink.sample.common.Event;

import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;

public class MakeCsv {
    public static void main(String[] args) throws InterruptedException {
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
            Thread.sleep(1000);
        }
        System.out.printf(line.toString());

        TimeZone timeZone = TimeZone.getDefault();
        System.out.println(timeZone.getDisplayName());
        System.out.println(timeZone.getID());
    }
}
