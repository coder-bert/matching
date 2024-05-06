package com.mybit.matching.core.disruptor;

import com.lmax.disruptor.EventFactory;
import com.mybit.matching.core.disruptor.DataEvent;

public class DataEventFactory implements EventFactory<DataEvent> {
    @Override
    public DataEvent newInstance() {
        return new DataEvent();
    }
}
