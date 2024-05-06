package com.mybit.matching.core.entity;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class CloseDumpEvent extends DumpEvent {
    private int timeout;
    private CountDownLatch closeLatch;

    public CloseDumpEvent(String dumpPath, long timestamp, int count, int timeout) {
        super(dumpPath, timestamp, count);

        this.timeout = timeout;
        this.closeLatch = new CountDownLatch(1);
    }

    public int getTimeout() { return timeout; }
    public CountDownLatch getCloseLatch() {
        return closeLatch;
    }
}
