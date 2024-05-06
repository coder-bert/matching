package com.mybit.matching.core.disruptor;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.kafka.KafkaFactory;
import com.mybit.matching.core.kafka.KafkaProducer;
import com.mybit.matching.core.orderbook.BaseEventHandler;
import com.mybit.matching.core.orderbook.OrderMatchingHandler;
import com.mybit.matching.core.sender.Sender;
import com.mybit.matching.core.sender.SenderFactory;
import com.mybit.matching.core.utils.threads.DefaultThreadFactory;
import com.mybit.matching.core.utils.Utils;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ThreadFactory;

public class DisruptorFactory {

    private static final DataEventFactory dataFactory = new DataEventFactory();

    public static Disruptor<DataEvent> newDisruptor(String symbol, int bufferSize, EventHandler<DataEvent> handler) {
        bufferSize = Utils.nearestPowerOfTwo(bufferSize);   // 2的次幂

        ThreadFactory threadFactory = new DefaultThreadFactory("disruptor-" + symbol);
        Disruptor<DataEvent> disruptor = new Disruptor<>(dataFactory, bufferSize,
                threadFactory, ProducerType.MULTI, new YieldingWaitStrategy());
        disruptor.handleEventsWith(handler);
        disruptor.start();

        return disruptor;
    }

    public static BaseEventHandler<Event> newHandler(String groupKey, TopicPartition tp) {
        // sender：向下游发送消息
        Config config = Config.getInstance();
        KafkaProducer kafkaProducer = KafkaFactory.newKafkaProducer(config);
        Sender sender = SenderFactory.createSender();
        sender.setKafkaProducer(kafkaProducer);
        return new OrderMatchingHandler(groupKey, tp, sender);
    }
}
