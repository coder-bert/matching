package com.mybit.matching.core;

import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.disruptor.GroupDisruptorManager;
import com.mybit.matching.core.dump.Dump;
import com.mybit.matching.core.kafka.KafkaConsumer;
import com.mybit.matching.core.kafka.KafkaFactory;
import com.mybit.matching.core.orderbook.OrderBookManager;
import com.mybit.matching.core.processor.KafkaDataProcessor;
import com.mybit.matching.core.processor.KafkaProcessorFactory;
import com.mybit.matching.core.utils.AssertUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *  基本流程：disruptor消息队列（kafka consumer -> parser -> disruptor -> matching -> sender
 */

@Slf4j
public class EngineServer implements InitializeAndCloseable {
    private static final Logger logger = LoggerFactory.getLogger(EngineServer.class);

    private volatile boolean isRunning = false;
    private volatile AtomicBoolean hasStarted = new AtomicBoolean(false);

    private KafkaConsumer kafkaConsumer = null;

    private final GroupDisruptorManager groupDisruptorManager = GroupDisruptorManager.getInstance();
    private final OrderBookManager orderBookManager = OrderBookManager.getInstance();

    private KafkaDataProcessor kafkaDataParser;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);


    private EngineServer() {}

    private static volatile EngineServer instance;
    public static EngineServer getInstance() {
        if (instance == null) {
            synchronized (EngineServer.class) {
                if (instance == null) {
                    instance = new EngineServer();
                }
            }
        }
        return instance;
    }


    @Override
    public boolean init() {
        Config config = Config.getInstance();

        // kafka consumer
        kafkaDataParser = KafkaProcessorFactory.createProcessor();
        kafkaConsumer = KafkaFactory.newKafkaConsumer(config);
        kafkaConsumer.setProcessor((tp, records, startTime) -> kafkaDataParser.onData(tp, records, startTime));
        kafkaConsumer.setInterval(config.getConsumeInterval());

        groupDisruptorManager.init();
        orderBookManager.init();

        // 加载Dump文件
        Dump.loadLastestDump();

        // 设置kafka起始消费位置
        Collection<TopicPartition> topicPartitions = orderBookManager.getTopicPartitions();
        Map<TopicPartition, Long> partitionOffsetMap = orderBookManager.getPartitionOffsetMap();
        kafkaConsumer.setStartConsumeTopicPartitionOffsets(topicPartitions, partitionOffsetMap);

        return true;
    }

    @Override
    public boolean startup() {
        if (hasStarted.compareAndExchange(false, true)) {
            logger.error("engineServer maybe have started");
            return false;
        }

        AssertUtil.notNull(kafkaConsumer, "kafkaConsumer is null");

        orderBookManager.startup();
        groupDisruptorManager.startup();
        kafkaConsumer.startup();

        isRunning = true;

        return true;
    }

    @Override
    public boolean close() {
        if (!hasStarted.compareAndExchange(true, false)) {
            logger.error("engineServer maybe not started");
            return false;
        }

        if (kafkaConsumer != null)
            kafkaConsumer.stop();
        groupDisruptorManager.close();

        shutdownLatch.countDown();
        isRunning = false;

        return true;
    }

    public void waitUntilStop() throws InterruptedException {
        int waitingCloseTimeout = Config.getInstance().getWaitingCloseTimeout();    // ms
        //synchronized (shutdownLatch) {
//            shutdownLatch.await(waitingCloseTimeout + 1000, TimeUnit.MILLISECONDS);
            shutdownLatch.await();
        //}
    }

    public boolean isRunning() {
        return this.isRunning;
    }
}
