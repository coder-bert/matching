
package com.mybit.matching.core.utils.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InnerThread extends Thread {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public static InnerThread daemon(String name, Runnable runnable) {
        return new InnerThread(name, runnable, true);
    }

    public static InnerThread nonDaemon(String name, Runnable runnable) {
        return new InnerThread(name, runnable, false);
    }

    public InnerThread(String name, boolean daemon) {
        super(name);
        this.configureThread(name, daemon);
    }

    public InnerThread(String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        this.configureThread(name, daemon);
    }

    private void configureThread(String name, boolean daemon) {
        this.setDaemon(daemon);
        this.setUncaughtExceptionHandler((t, e) -> {
            this.log.error("Uncaught exception in thread '{}':", name, e);
        });
    }
}
