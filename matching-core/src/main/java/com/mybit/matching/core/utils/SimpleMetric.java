package com.mybit.matching.core.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class SimpleMetric {
    public static void printQps(String prefix, AtomicLong totalTime, AtomicLong totalRecords, AtomicLong totalCount, long startTime, long size) {
        long delta = System.nanoTime() - startTime;
        long total = totalTime.addAndGet(delta);
        long records = totalRecords.addAndGet(size);
        long count = totalCount.incrementAndGet();
        if (records > 0 && (count % 1_000 == 0 || size == 0)) {
            log.info("{}: {}/{}={} {}", prefix, total, records, 1000_000_000L / (total / records), delta / 1000000);
        }
    }
}
