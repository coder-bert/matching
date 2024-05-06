package com.mybit.matching.core.utils.threads;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ScheduledThreadPool {

        private final int poolSize;
        private final ScheduledThreadPoolExecutor scheduledThreadPool;

        public ScheduledThreadPool(int poolSize) {
            this.poolSize = poolSize;
            scheduledThreadPool = createPool();
        }

        private ScheduledThreadPoolExecutor createPool() {
            ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(poolSize);
            threadPool.setRemoveOnCancelPolicy(true);
            threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            threadPool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            threadPool.setThreadFactory(new ThreadFactory() {
                private final AtomicInteger sequence = new AtomicInteger();

                public Thread newThread(Runnable r) {
                    return InnerThread.daemon("kafka-rlm-thread-pool-" + sequence.incrementAndGet(), r);
                }
            });

            return threadPool;
        }

        public Double getIdlePercent() {
            return 1 - (double) scheduledThreadPool.getActiveCount() / (double) scheduledThreadPool.getCorePoolSize();
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {
            log.info("Scheduling runnable {} with initial delay: {}, fixed delay: {}", runnable, initialDelay, delay);
            return scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
        }

        public void close() {
            shutdownAndAwaitTermination(scheduledThreadPool, "RLMScheduledThreadPool", 10, TimeUnit.SECONDS);
        }

    private static void shutdownAndAwaitTermination(ExecutorService pool, String poolName, long timeout, TimeUnit timeUnit) {
        // This pattern of shutting down thread pool is adopted from here: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html
        log.info("Shutting down of thread pool {} is started", poolName);
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeout, timeUnit)) {
                log.info("Shutting down of thread pool {} could not be completed. It will retry cancelling the tasks using shutdownNow.", poolName);
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeout, timeUnit))
                    log.warn("Shutting down of thread pool {} could not be completed even after retrying cancellation of the tasks using shutdownNow.", poolName);
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            log.warn("Encountered InterruptedException while shutting down thread pool {}. It will retry cancelling the tasks using shutdownNow.", poolName);
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        log.info("Shutting down of thread pool {} is completed", poolName);
    }
}