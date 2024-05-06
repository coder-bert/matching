package com.mybit.matching.core.kafka;

import com.mybit.matching.core.config.Config;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;


@Slf4j
public class KafkaFactory {

    public static KafkaConsumer newKafkaConsumer(Config config) {
        Properties props = config.getKafkaConsumerProps();
        return new KafkaConsumer(props);
    }

    public static KafkaProducer newKafkaProducer(Config config) {
        Properties props = config.getKafkaProducerProps();
        String topic = config.getProducerTopic();
        return new KafkaProducer(props, topic);
    }
}
