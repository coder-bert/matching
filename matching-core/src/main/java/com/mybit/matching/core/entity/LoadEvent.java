package com.mybit.matching.core.entity;


import com.mybit.matching.core.orderbook.FileNameRecord;
import com.mybit.matching.core.orderbook.TopicPartitionOffset;
import lombok.Data;

import java.util.List;

@Data
public class LoadEvent extends Event {

    private TopicPartitionOffset topicPartitionOffset;
    private List<FileNameRecord> fileNameRecords;
    public LoadEvent(TopicPartitionOffset topicPartitionOffset, List<FileNameRecord> fileNameRecords) {
        this.topicPartitionOffset = topicPartitionOffset;
        this.fileNameRecords = fileNameRecords;
    }
}
