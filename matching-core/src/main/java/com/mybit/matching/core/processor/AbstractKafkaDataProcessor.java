package com.mybit.matching.core.processor;

import com.mybit.matching.core.disruptor.GroupDisruptor;
import com.mybit.matching.core.disruptor.GroupDisruptorManager;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.SimpleMetric;
import com.mybit.matching.core.utils.StringUtil;
import com.mybit.matching.core.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class AbstractKafkaDataProcessor implements KafkaDataProcessor {

    private final GroupDisruptorManager groupDisruptorManager = GroupDisruptorManager.getInstance();

    private AtomicLong totalTime = new AtomicLong(0);
    private AtomicLong totalRecords = new AtomicLong(0);
    private AtomicLong totalCount = new AtomicLong(0);

    /**
     * 协议解析，当前实现了json解析
      */
    public abstract Event parse(String groupKey, ConsumerRecord<String, String> record, long startTime);

    /**
     * kafka消费者调用
     *
     *  已经按照topic+partition分组了，这里是一个tp的记录
     *
     * @param tp
     * @param records
     * @param startTime
     * @return
     */
    @Override
    public boolean onData(TopicPartition tp, Collection<ConsumerRecord<String, String>> records, long startTime) {
        if (records == null) return true;

        String groupKey = tp.toString();    // 根据分区并发，其它并发可能有问题

        // 已经按照topic+partition分组了
        List<Event> events = records.stream().map(record -> this.parse(groupKey, record, startTime)).toList();

        GroupDisruptor groupDisruptor = groupDisruptorManager.getDisruptorOrCreate(groupKey, tp);
        if (groupDisruptor == null) {
            // 不应该执行到这里
            AssertUtil.isTrue(false, "groupKey not exist and create failed " + groupKey);
            return false;
        }

        // 发布到撮合引擎队列
        //  在优雅退出时，下游close后会返回错误【可忽略】，其它情况发送时，如果queue满了后会阻塞，相当于流控了
        if (!groupDisruptor.fireEvents(groupKey, events, startTime)) {
            log.error("parser publish failed {} {}", groupKey, events.size());
            return false;
        }

        SimpleMetric.printQps("parser qps", totalTime, totalRecords, totalCount, startTime, events.size());

        return true;
    }

    /**
     // 根据header中KEY或者record key进行分组，分组内顺序、分组间并发，都没有设置时用topic+partition分组
     *
     * @param record
     * @return
     */
    private String parseConcurrentKey(ConsumerRecord<String, String> record) {
        String key = null;

        // BUG: 使用header或者key可能存在同一个tp分到不同的线程并发处理，导致tp的offset不一定满足顺序性，“续传”无法保证。
        Header keyHeader = record.headers().lastHeader("KEY");
        if (keyHeader != null) {
            key = Utils.utf8(keyHeader.value());
        }

        if (StringUtil.isEmpty(key)) {
            key = record.key();
        }

        if (StringUtil.isEmpty(key)) {
            key = String.format("%s-%d", record.topic(), record.partition());
        }
        return key;
    }

    /**
     * 解析交易对： 1. SYMBOL header   2. key
     *
     * @param record
     * @return
     */
    public static String getSymbol(ConsumerRecord<String, String> record) {
        String key = null;
        Header keyHeader = record.headers().lastHeader("SYMBOL");
        if (keyHeader != null) {
            key = Utils.utf8(keyHeader.value());
        }
        if (StringUtil.isEmpty(key)) {
            key = record.key();
        }
        return key;
    }
}