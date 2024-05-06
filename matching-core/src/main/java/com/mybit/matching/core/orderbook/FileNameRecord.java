package com.mybit.matching.core.orderbook;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FileNameRecord {
    long timestamp;
    String topic;
    int partition;
    long matchedOffset;
    long sentOffset;

    String dstTopic;
    int dstPartition;
    long dstSentOffset;

    int num;

    String fileName;
}