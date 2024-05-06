package com.mybit.matching.core.processor;


import com.alibaba.fastjson2.JSON;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class KafkaJsonDataProcessor extends AbstractKafkaDataProcessor {
    @Override
    public Event parse(String groupKey, ConsumerRecord<String, String> record, long startTime) {
        OrderEvent orderEvent = JSON.parseObject(record.value(), OrderEvent.class);
        orderEvent.setStartTime(startTime);

        orderEvent.setSymbol(getSymbol(record));
        orderEvent.setGroupKey(groupKey);

        orderEvent.setTopic(record.topic());
        orderEvent.setPartition(record.partition());
        orderEvent.setOffset(record.offset());
        orderEvent.setKey(record.key());
        orderEvent.setHeaders(orderEvent.getHeaders());

        return orderEvent;
    }
}