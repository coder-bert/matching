package com.mybit.matching.core.kafka;

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Maps;
import com.mybit.matching.core.processor.KafkaDataProcessor;
import com.mybit.matching.core.orderbook.OrderBookManager;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.SimpleMetric;
import com.mybit.matching.core.utils.threads.DefaultThreadFactory;
import com.mybit.matching.core.utils.PropertyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class KafkaConsumer implements ConsumerRebalanceListener {

    private String bootstrapServers = "localhost:9092";
    private boolean autoCommitted = true;
    private String groupId = "matchingGroupId";
    private String autoOffsetRest = "earliest";
    private Properties props;

    private AtomicLong received = new AtomicLong(0);
    private AtomicLong totalTime = new AtomicLong(0);
    private AtomicLong totalRecords = new AtomicLong(0);
    private AtomicLong totalCount = new AtomicLong(0);

    private Map<TopicPartition, Long> firstConsumedOffsets = Maps.newHashMap();

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> delegating;
    private Thread consumerThread;

    // 指定消费的topic和开始消费位置（必须）
    private Collection<TopicPartition> topicPartitions;
    private Map<TopicPartition, Long> partitionOffsetMap;

    private KafkaDataProcessor kafkaDataParser;

    private final OrderBookManager orderBookManager = OrderBookManager.getInstance();

    private volatile boolean isRunning = false;
    private volatile boolean stopped = false;
    private long interval = 5;      // seconds

    public KafkaConsumer(String bootstrapServers,
                    String groupId,
                    boolean autoCommitted,
                    String autoOffsetRest) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.autoCommitted = autoCommitted;
        this.autoOffsetRest = autoOffsetRest;
        this.props = kafkaProps();
    }

    public KafkaConsumer(Properties props) {
        this.props = PropertyUtils.merge(props, kafkaProps());
        this.bootstrapServers = this.props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
        this.groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    public KafkaConsumer() {
        this.props = kafkaProps();
    }

    private Properties kafkaProps() {
        Properties props = new Properties();
        // bootstrap server config is required for consumer to connect to brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // consumer group id is required when we use subscribe(topics) for group management
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        props.put(ProducerConfig.CLIENT_ID_CONFIG, String.format("consumer-%s-%d", groupId, Thread.currentThread().getId()));
        // sets static membership to improve availability (e.g. rolling restart)
//        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId);
        // disables auto commit when EOS is enabled, because offsets are committed with the transaction
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommitted ? "true" : "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);
        // key and value are just byte arrays, so we need to set appropriate deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (autoCommitted) {
            // skips ongoing and aborted transactionsqq
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        // sets the reset offset policy in case of invalid or no offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetRest); // earliest

        return props;
    }

    public boolean startup() {
        isRunning = true;

        // kafkaConsumer是非线程安全的，一个consumer只能启动一个线程
        DefaultThreadFactory defaultThreadFactory = new DefaultThreadFactory("Consumer");
        consumerThread = defaultThreadFactory.newThread(this::reConsume);
        consumerThread.start();

        return true;
    }

    // 指定消费位置
    public void setStartConsumeTopicPartitionOffsets(Collection<TopicPartition> topicPartitions, Map<TopicPartition, Long> partitionOffsetMap) {
        AssertUtil.notEmpty(topicPartitions, "invalid topicPartitions");
        //AssertUtil.notEmpty(partitionOffsetMap, "invalid partitionOffsetMap");

        this.topicPartitions = topicPartitions;
        this.partitionOffsetMap = partitionOffsetMap;
    }

    private void reConsume() {
        while(isRunning) {
            if (!consume()) {
                // 非正常退出，重新消费
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        stopped = true;
    }

    /**
     * 指定消费位置
     */
    private boolean consume() {
        boolean success = true;

        initKafkaConsumer();

        AssertUtil.notNull(delegating, "delegating is null");
        AssertUtil.notNull(kafkaDataParser, "processor is null");
        AssertUtil.notEmpty(topicPartitions, "topicPartitions is null");
        //AssertUtil.notEmpty(partitionOffsetMap, "partitionOffsetMap is null");

        stopped = false;
        log.info("consumer start {} {} {} {}", groupId, bootstrapServers, topicPartitions, partitionOffsetMap);

        // subscribes to a list of topics to get dynamically assigned partitions
        // this class implements the rebalance listener that we pass here to be notified of such events
//        delegating.subscribe(singleton(topic), this);
        delegating.assign(topicPartitions);        // 手动订阅（避免了同coordinator的join和sync交互），且指定开始消费的offsets
        if (partitionOffsetMap != null && !partitionOffsetMap.isEmpty()) {
            partitionOffsetMap.forEach((tp, offset) -> {
                if (offset == null || offset < 0) {
                    // 根据自动提交的offset消费 或 reset机制
                    //delegating.seekToBeginning(Collections.singleton(tp));
                } else {
                    delegating.seek(tp, offset);
                }
            });
        }

        while (isRunning) {
            try {
                long startTime = System.nanoTime();

                // if required, poll updates partition assignment and invokes the configured rebalance listener
                // then tries to fetch records sequentially using the last committed offset or auto.offset.reset policy
                // returns immediately if there are records or times out returning an empty record set
                // the next poll must be called within session.timeout.ms to avoid group rebalance
                ConsumerRecords<String, String> records = delegating.poll(Duration.ofSeconds(interval));
                if (records.isEmpty()) {
                    kafkaDataParser.onData(null, null, startTime);
                    continue;
                }

                // 按照topicPartition消费
                Set<TopicPartition> partitions = records.partitions();
                partitions.forEach(tp -> {
                    List<ConsumerRecord<String, String>> partRecords = records.records(tp);
                    kafkaDataParser.onData(tp, partRecords, startTime);

                    SimpleMetric.printQps("kafka consumer recv qps", totalTime, totalRecords, totalCount, startTime, partRecords.size());

                    if (!firstConsumedOffsets.containsKey(tp)) {
                        // 每次启动第一条数据打印offset，方便查看“续传”
                        long offset = partRecords.iterator().next().offset();
                        log.info("the first offset consumed {} {}", tp.toString(), offset);
                        firstConsumedOffsets.put(tp, offset);
                    }
                });
            } catch (AuthorizationException | UnsupportedVersionException | RecordDeserializationException e) {
                // we can't recover from these exceptions
                log.error("{}:", e.getClass(), e);
                success = false;
                break;  // 不可恢复，退出循环，重新创建consumer
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                // invalid or no offset found without auto.reset.policy
                log.error("{}: Invalid or no offset found, using latest", e.getClass(), e);
                delegating.seekToEnd(e.partitions());
                delegating.commitSync();
            } catch (KafkaException e) {
                // log the exception and try to continue
                log.error("{}:", e.getClass(), e);
            } catch (Exception e) {
                log.error("{}:", e.getClass(), e);
            }
        } // end of while(isRunning)

        delegating.close(Duration.ofSeconds(5));

        log.warn("kafka consumer closed!");

        stopped = true;

        return success;
    }


    public void pause() {
        AssertUtil.notNull(delegating, "delegating is null");
        delegating.pause(topicPartitions);
    }

    public void resume() {
        AssertUtil.notNull(delegating, "delegating is null");
        delegating.resume(topicPartitions);
    }

    public void stop() {
        isRunning = false;
    }

    public boolean isStopped() {
        return stopped;
    }

    public void initKafkaConsumer() {
        if (delegating != null) return;
        delegating = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        Assert.isTrue(delegating != null, "create kafka consumer failed " + props);
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Revoked partitions: {}", partitions);

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("assigned partitions: {}", partitions);

    }

    public void setProcessor(KafkaDataProcessor kafkaDataParser) {
        this.kafkaDataParser = kafkaDataParser;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
        if (interval == null || interval <= 0)
            this.interval = 5;
    }
}
