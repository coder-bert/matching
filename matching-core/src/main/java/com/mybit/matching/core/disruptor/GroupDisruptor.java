package com.mybit.matching.core.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.entity.CloseDumpEvent;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.RegularDumpEvent;
import com.mybit.matching.core.orderbook.BaseEventHandler;
import com.mybit.matching.core.orderbook.OrderMatchingHandler;
import com.mybit.matching.core.utils.CollectionUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;

@Slf4j
@Data
public class GroupDisruptor implements InitializeAndCloseable {
    private String groupKey;
    private Disruptor<DataEvent> disruptor;
    private RingBuffer<DataEvent> ringBuffer;
    private BaseEventHandler<Event> handler;

    private volatile boolean isRunning = false;    // 关闭时设置，不再允许外部fire events


    public GroupDisruptor(String groupKey, Disruptor<DataEvent> disruptor,
                          BaseEventHandler<Event> handler) {
        this.groupKey = groupKey;
        this.disruptor = disruptor;
        this.ringBuffer = disruptor.getRingBuffer();
        this.handler = handler;
    }


    @Override
    public boolean init() {
        if (isRunning) return false;
        handler.init();
        return true;
    }

    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;
        handler.startup();
        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning) return false;
        isRunning = false;
        //handler.close();      // 通过发送CloseEvent进行dump和close
        return true;
    }

    public boolean fireEvents(String groupKey, Collection<Event> events, long startTime) {
        if (!isRunning) return false;   // 不再允许fire events
        return innerFireEvents(groupKey, events, startTime);
    }

    private boolean innerFireEvents(String groupKey, Collection<Event> events, long startTime) {
        if (CollectionUtil.isEmpty(events)) return false;

        //log.info("inner fireEvents {} @{} {}", groupKey, System.identityHashCode(events.stream().findFirst().get()), startTime);

        long sequence = ringBuffer.next();
        try {
            DataEvent event = ringBuffer.get(sequence);
            event.setEvents(groupKey, events, startTime);
            return true;
        } catch (Exception e) {
            log.error("publish event exception {} {}", groupKey, events.size(), e);
            return false;
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    private void fireEvent(Event event) {
        innerFireEvents(groupKey, Collections.singleton(event), System.nanoTime());
    }

    public void fireRegularDumpEvent(RegularDumpEvent event) {
        fireEvent(event);
    }

    public void closeAndFireDumpEvent(CloseDumpEvent event) {
        close();
        fireEvent(event);
    }



    /**
     * for test only
     */
    public boolean isDiscard(String symbol, long offset) {
        return ((OrderMatchingHandler)handler).isDiscard(symbol, offset);
    }
    public long fistDiscardOffset(String symbol, long offset) {
        return ((OrderMatchingHandler)handler).firstDiscardOffset(symbol, offset);
    }

}

