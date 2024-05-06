package com.mybit.matching.core.entity;

import lombok.Data;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


@Data
public class DumpEvent extends Event {

    private int count;              // 并发度
    private String dumpPath;
    private Long timestamp;
    private CountDownLatch orderBookDumpLatch;
    private Set<String> symbols;

    private volatile AtomicBoolean offsetDumpLock = new AtomicBoolean(false);
    private volatile AtomicBoolean contExecLock = new AtomicBoolean(false);

    public DumpEvent(String dumpPath, long timestamp, int count) {
        this.count = count;
        this.dumpPath = dumpPath;
        this.timestamp = timestamp;
        this.orderBookDumpLatch = new CountDownLatch(count);
    }

}
