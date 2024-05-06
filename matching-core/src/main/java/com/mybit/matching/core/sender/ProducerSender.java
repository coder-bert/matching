package com.mybit.matching.core.sender;

import com.alibaba.fastjson2.JSON;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.OrderResult;
import com.mybit.matching.core.exception.ClosedException;
import com.mybit.matching.core.kafka.KafkaProducer;
import com.mybit.matching.core.orderbook.OrderBookManager;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.threads.DefaultThreadFactory;
import com.mybit.matching.core.utils.threads.ExtExecutors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


@Slf4j
public class ProducerSender implements Sender {

    private OrderBookManager orderBookManager = OrderBookManager.getInstance();
    private Config config = Config.getInstance();

    private KafkaProducer kafkaProducer;

    private ExecutorService executorService = null;
    private boolean isRunning = false;

    public ProducerSender() {

        // 如果配置了异步发送，且设置了(>0)重试队列长度，开启重试线程池（单线程），发送失败的消息
        boolean sendAsync = config.isSendAsync();
        int retryQueueCapacity = config.getRetryQueueCapacity();
        if (retryQueueCapacity > 0 && sendAsync) {
            executorService = ExtExecutors.newSingleThreadExecutor(retryQueueCapacity, new DefaultThreadFactory("send-retry"));
        }
    }

    @Override
    public void setKafkaProducer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    /**
     * kafka同步发送数据（单个）
     *
     * @param orderEvent
     * @param orderResult
     * @return 成功发送后返回kafka offset，失败时为-1
     */
    @Override
    public long sendOne(Event orderEvent, OrderResult orderResult) {
        RecordMetadata recordMetadata = fireSync(orderEvent, orderResult);
        if (recordMetadata != null && recordMetadata.hasOffset()) {
            // 发送成功时，更新发送进度（offset）
            TopicPartition srcTp = new TopicPartition(orderEvent.getTopic(), orderEvent.getPartition());
            TopicPartition dstTp = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
            orderBookManager.updateSuccessSentOffset(srcTp, orderEvent.getOffset(), dstTp, recordMetadata.offset());  // 分别是源kafka的和目标kafka的topic+offset
        }

        return recordMetadata != null ? recordMetadata.offset() : -1;
    }

    /**
     * kafka同步发送数据（批量）
     *
     * @param groupKey
     * @param orderResults
     * @param kafkaTopic
     * @param partition
     * @param maxOffsetInBatch
     * @return
     */
    @Override
    public long sendBatch(String groupKey, List<OrderResult> orderResults, String kafkaTopic, int partition, Long maxOffsetInBatch) {
        RecordMetadata recordMetadata = fireSyncBatch(groupKey, orderResults, kafkaTopic, partition, maxOffsetInBatch);
        if (recordMetadata != null && recordMetadata.hasOffset()) {
            // 发送成功时，更新发送进度（offset）
            TopicPartition srcTp = new TopicPartition(kafkaTopic, partition);
            TopicPartition dstTp = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
            orderBookManager.updateSuccessSentOffset(srcTp, maxOffsetInBatch, dstTp, recordMetadata.offset());  // 分别是源kafka的和目标kafka的topic+offset
        }

        return recordMetadata != null ? recordMetadata.offset() : -1;
    }

    /**
     * kafka异步发送数据（批量）
     *
     * @param groupKey
     * @param orderResults
     * @param kafkaTopic
     * @param partition
     * @param maxOffsetInBatch
     * @param callback
     * @return
     */
    @Override
    public long sendBatch(String groupKey, List<OrderResult> orderResults, String kafkaTopic, int partition, Long maxOffsetInBatch, int curRetyTimes, Callback callback) {
        fireAsyncBatch(groupKey, orderResults, (recordMetadata, e) -> {
            if (e != null) {
                if (!(e instanceof ClosedException)) { // producer closed
                    // 失败重试，控制重试次数
                    int RETY_TIMES = config.getSendRetryTimes();       // -1: 防止丢数据无限重试
                    if ((RETY_TIMES < 0 || curRetyTimes < RETY_TIMES) && executorService != null) {
                        try {
                            Future<?> future = executorService.submit(() -> {
                                sendBatch(groupKey, orderResults, kafkaTopic, partition, maxOffsetInBatch, curRetyTimes + 1, callback);
                            });

                            log.warn("kafka async send failed, retry... {} {} {} {} {}/{}", groupKey, kafkaTopic, partition, maxOffsetInBatch, curRetyTimes + 1, RETY_TIMES);
                            return;
                        } catch (Exception ex) {
                            log.error("kafka async send failed, submit retry task failed, data will be dropped. {} {} {} {}", groupKey, kafkaTopic, partition, maxOffsetInBatch, ex);
                        }
                    } else {
                        log.error("kafka async send failed. {} {} {} {} {} {}", groupKey, kafkaTopic, partition, maxOffsetInBatch, JSON.toJSONString(orderResults), curRetyTimes);
                    }
                }
            } else {
                // 发送成功时，更新发送进度（offset）
                TopicPartition srcTp = new TopicPartition(kafkaTopic, partition);
                TopicPartition dstTp = new TopicPartition(recordMetadata.topic(), recordMetadata.partition());
                orderBookManager.updateSuccessSentOffset(srcTp, maxOffsetInBatch, dstTp, recordMetadata.offset());  // 分别是源kafka的和目标kafka的topic+offset
            }

            callback.onCompletion(recordMetadata, e);
        });
        return -1; // no sense
    }

    private RecordMetadata fireSync(Event orderEvent, OrderResult orderResult) {
        AssertUtil.notNull(kafkaProducer, "producer is null");
        AssertUtil.notNull(orderResult, "invalid result: orderResult is null");

        final String seq = orderEvent.getSeq();
        final String groupKey = orderEvent.getGroupKey();
        final String data = JSON.toJSONString(List.of(orderResult));

        int RETY_TIMES = config.getSendRetryTimes();       // -1: 防止丢数据无限重试，会阻塞后续操作
        int retry = 0;
        while (retry++ < RETY_TIMES) {
            try {
                RecordMetadata metadata = kafkaProducer.syncSend(groupKey, data);
                if (metadata != null) {
                    return metadata;
                }
            } catch (ClosedException e) {
                log.error("producer closed {} {}", seq, groupKey, e);
                break;  // producer关闭，退出重试
            } catch (ExecutionException | InterruptedException e) {
                log.error("exception {} {}", seq, groupKey, e);
            }

            log.warn("kafka sync send failed, retrying... {} {} {} {} {} {}/{}", seq, groupKey, orderEvent.getTopic(), orderEvent.getPartition(), orderEvent.getOffset(), retry, RETY_TIMES);

            // 不需要backoff，kafka发送内部会排队等待max.block.ms时间超时
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        log.error("kafka sync send failed. {} {} {} {} {} {}/{}", seq, groupKey, orderEvent.getTopic(), orderEvent.getPartition(), orderEvent.getOffset(), retry, RETY_TIMES);

        return null;
    }


    private RecordMetadata fireSyncBatch(String groupKey, List<OrderResult> orderResults, String kafkaTopic, int partition, Long maxOffsetInBatch) {
        AssertUtil.notNull(kafkaProducer, "producer is null");
        AssertUtil.notEmpty(orderResults, "invalid result: orderResults is empty");

        final String data = JSON.toJSONString(orderResults);

        int RETY_TIMES = config.getSendRetryTimes();       // -1: 防止丢数据无限重试，会阻塞后续操作
        int retry = 0;
        while (retry++ < RETY_TIMES) {
            try {
                RecordMetadata metadata = kafkaProducer.syncSend(groupKey, data);
                if (metadata != null) {
                    //log.info("sent {} {} {}", groupKey, maxOffsetInBatch, metadata.offset());
                    return metadata;
                }
            } catch (ClosedException e) {
                log.error("producer closed {} {} {} {} {}", groupKey, kafkaTopic, partition, maxOffsetInBatch, groupKey, e);
                break;  // producer关闭，退出重试
            } catch (ExecutionException | InterruptedException e) {
                log.error("exception {} {} {} {} {}", groupKey, kafkaTopic, partition, maxOffsetInBatch, groupKey, e);
            }

            log.warn("kafka sync send failed, retrying... {} {} {} {} {} {}/{}", groupKey, groupKey, kafkaTopic, partition, maxOffsetInBatch, retry, RETY_TIMES);

            // 一般不需要backoff，kafka发送内部会排队等待超时。
            // 但是当kkafka down机时会立即返回
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        log.error("kafka sync send failed. {} {} {} {} {} {}/{}", groupKey, groupKey, kafkaTopic, partition, maxOffsetInBatch, retry, RETY_TIMES);

        return null;
    }


    private Future<?> fireAsyncBatch(String groupKey, List<OrderResult> orderResults, Callback callback) {
        AssertUtil.notNull(kafkaProducer, "producer is null");
        AssertUtil.notEmpty(orderResults, "invalid result: orderResults is empty");

        final String data = JSON.toJSONString(orderResults);

        try {
            Future<RecordMetadata> recordMetadataFuture = kafkaProducer.asyncSend(groupKey, data, callback);
            if (recordMetadataFuture.isDone()) {
                try {
                    recordMetadataFuture.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            return recordMetadataFuture;

        } catch (ClosedException e) {
            callback.onCompletion(null, e);

            return null;    // null?
        }
    }

    @Override
    public boolean init() {
        AssertUtil.notNull(kafkaProducer, "producer is null");
        kafkaProducer.init();
        return true;
    }

    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;

        AssertUtil.notNull(kafkaProducer, "producer is null");
        kafkaProducer.startup();
        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning) return false;
        isRunning = false;
        AssertUtil.notNull(kafkaProducer, "producer is null");
        kafkaProducer.close();
        return true;
    }

    @Override
    public boolean flush() {
        AssertUtil.notNull(kafkaProducer, "producer is null");
        kafkaProducer.flush();
        return true;
    }

}
