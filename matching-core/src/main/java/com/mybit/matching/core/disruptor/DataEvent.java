package com.mybit.matching.core.disruptor;

import com.mybit.matching.core.entity.Event;

import java.util.Collection;

public class DataEvent {
    private Long startTime;                         // 纳秒
    private String groupKey;
    private Collection<Event> events;               // 所有事件的基类

    public void setEvents(String groupKey, Collection<Event> orderEvents, long startTime) {
        this.groupKey = groupKey;
        this.events = orderEvents;
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getGroupKey() {
        return groupKey;
    }

    public Collection<Event> getEvents() {
        return events;
    }
}
