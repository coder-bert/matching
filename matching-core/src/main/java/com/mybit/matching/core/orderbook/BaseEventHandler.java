package com.mybit.matching.core.orderbook;

import com.lmax.disruptor.EventHandler;
import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.disruptor.DataEvent;
import com.mybit.matching.core.entity.Event;

import java.util.Collection;

public interface BaseEventHandler<T> extends EventHandler<DataEvent>, InitializeAndCloseable {
    boolean onEvent(String groupKey, Collection<Event> orderEvents, long createTime);
}