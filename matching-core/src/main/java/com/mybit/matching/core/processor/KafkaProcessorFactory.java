package com.mybit.matching.core.processor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaProcessorFactory {

    public static KafkaDataProcessor createProcessor() {
        return new KafkaJsonDataProcessor();    // 不同的processor支持不同的协议
    }
}
