package com.mybit.matching.core.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public interface KafkaDataProcessor {

    /**
     * kafka消费到数据后交由processor处理
     *  由实现者保证处理成功，且不抛出异常
     * @param tp
     * @param records
     * @param startTime
     * @return true: 处理成功 false：失败
     */
    boolean onData(TopicPartition tp, Collection<ConsumerRecord<String, String>> records, long startTime);
}
