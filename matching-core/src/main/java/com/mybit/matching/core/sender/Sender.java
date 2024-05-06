package com.mybit.matching.core.sender;

import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.OrderResult;
import org.apache.kafka.clients.producer.Callback;
import com.mybit.matching.core.kafka.KafkaProducer;

import java.util.List;

public interface Sender extends InitializeAndCloseable {

    /**
     * 下游
     * @param kafkaProducer
     */
    void setKafkaProducer(KafkaProducer kafkaProducer);

    /**
     * kafka发送数据
     *
     * @param orderEvent
     * @param orderResult
     * @return 成功发送后返回kafka offset，失败时为-1
     */
    long sendOne(Event orderEvent, OrderResult orderResult);

    /**
     * 批量模式
     * @param orderResults
     * @return
     */
    long sendBatch(String groupKey, List<OrderResult> orderResults, String kafkaTopic, int partition, Long maxOffsetInBatch);

    /**
     * 异步+批量发送数据
     *
     * @param groupKey
     * @param orderResults
     * @param callback
     * @return
     */
    long sendBatch(String groupKey, List<OrderResult> orderResults, String kafkaTopic, int partition, Long offset, int curRetyTimes, Callback callback);


}
