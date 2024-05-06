package com.mybit.matching.core.kafka;

import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.exception.ClosedException;
import com.mybit.matching.core.utils.PropertyUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KafkaProducer implements InitializeAndCloseable {
    private String bootstrapServers = "localhost:9092";
    private Properties props;
    private String topic;
    private boolean isRunning = false;

    private static AtomicInteger clientIdIncr = new AtomicInteger(0);


    private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = null;

    public KafkaProducer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.props = defaultProps();
    }

    public KafkaProducer(Properties props, String topic) {
        this.topic = topic;
        this.props = PropertyUtils.merge(props, defaultProps());
    }


    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;

        log.info("create kafkaConsumer {}", props);
        if (props.containsKey(ProducerConfig.CLIENT_ID_CONFIG)) {
            String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, String.format("%s-%d", clientId, clientIdIncr.getAndIncrement()));
        }
        producer = createKafkaProducer(props);

        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning) return false;
        isRunning = false;
        producer.close(Duration.ofSeconds(5));
        return true;
    }

    @Override
    public boolean flush() {
        if (!isRunning) return false;
        producer.flush();
        return true;
    }


    private org.apache.kafka.clients.producer.KafkaProducer<String, String> createKafkaProducer(Properties props) {
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    private Properties defaultProps() {
        Properties props = new Properties();
        // bootstrap server config is required for producer to connect to brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        props.put(ProducerConfig.CLIENT_ID_CONFIG, String.format("producer-%s-%d", topic, Thread.currentThread().getId()));
        // key and value are just byte arrays, so we need to set appropriate serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);                                 // default: 0, 立即发送
        props.put(ProducerConfig.ACKS_CONFIG, "all");                                  // default: -1 or all，可靠发送
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60 * 1000);                      // default: 60 * 1000
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);                   // default: Integer.MAX_VALUE
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.LZ4.name);   // default: None
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_000);                           // default: 16k
//        if (transactionTimeoutMs > 0) {
//            // max time before the transaction coordinator proactively aborts the ongoing transaction
//            props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
//        }
//        if (transactionalId != null) {
//            // the transactional id must be static and unique
//            // it is used to identify the same producer instance across process restarts
//            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
//        }
        // enable duplicates protection at the partition level
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);                   // default: true, 实现消息幂等性
        return props;
    }


    public Future<RecordMetadata> asyncSend(String key, String value, Callback callback) throws ClosedException {
        return asyncSend(topic, key, value, callback);
    }

    public Future<RecordMetadata> asyncSend(String topic, String key, String value, Callback callback) throws ClosedException {
        checkClose();
        // send the record asynchronously, setting a callback to be notified of the result
        // note that, even if you set a small batch.size with linger.ms=0, the send operation
        // will still be blocked when buffer.memory is full or metadata are not available
        return producer.send(new ProducerRecord<>(topic, key, value), callback);
    }

    public RecordMetadata syncSend(String key, String value) throws ExecutionException, InterruptedException, ClosedException {
        return syncSend(topic, key, value);
    }

    public RecordMetadata syncSend(String topic, String key, String value) throws ExecutionException, InterruptedException, ClosedException {
        checkClose();
        try {
            // send the record and then call get, which blocks waiting for the ack from the broker
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, key, value)).get();
//            log.info("send finished {} {} {} {}", topic, key, value, metadata.offset());
            return metadata;
        } catch (AuthorizationException | UnsupportedVersionException | ProducerFencedException
                 | FencedInstanceIdException | OutOfOrderSequenceException | SerializationException e) {
            log.error("syncSend failed {} {} {}", topic, key, value, e);
            // we can't recover from these exceptions
//            close(); // todo re-connect
        } catch (RecordTooLargeException e) {
            log.error("syncSend failed {} {} {}", topic, key, value, e);
        } catch (KafkaException e) {
            log.error("syncSend failed {} {} {}", topic, key, value, e);
        }
        return null;
    }

    private void checkClose() throws ClosedException {
        if (!isRunning)
            throw new ClosedException("kafkaProducer has closed!");
    }
}
