package com.flink.sample.common;

import java.sql.Timestamp;

public class ClickEvent {
    public String user;
    public String url;
    public Long ts;
    public ClickEvent() {
    }
    public ClickEvent(String user, String url, Long ts) {
        this.user = user;
        this.url = url;
        this.ts = ts;
    }
    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
