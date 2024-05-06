package com.mybit.matching.core.entity;

import java.util.Set;

public class RegularDumpEvent extends DumpEvent {
    public RegularDumpEvent(String dumpPath, long timestamp, int count) {
        super(dumpPath, timestamp, count);
    }
}
