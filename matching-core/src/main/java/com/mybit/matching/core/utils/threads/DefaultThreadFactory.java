package com.mybit.matching.core.utils.threads;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultThreadFactory implements ThreadFactory {
    private static final AtomicInteger poolNumber = new AtomicInteger(0);
    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final ThreadGroup group;
    private final String namePrefix;

    private final boolean deamon;

    private int priority = Thread.NORM_PRIORITY + 2;

    public DefaultThreadFactory() {
        this("pool-" + poolNumber.getAndIncrement(), false);
    }
    public DefaultThreadFactory(String namePrefix) {
        this(namePrefix + "-" + poolNumber.getAndIncrement(), false);
    }
    public DefaultThreadFactory(String namePrefix, boolean daemon) {
        this.group = Thread.currentThread().getThreadGroup();
        this.namePrefix = namePrefix + "-";
        this.deamon = daemon;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        if (t.isDaemon())
            t.setDaemon(this.deamon);
        if (t.getPriority() != priority)
            t.setPriority(priority);
        return t;
    }
}