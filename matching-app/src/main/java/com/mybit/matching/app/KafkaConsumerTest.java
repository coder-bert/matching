package com.mybit.matching.app;

import com.alibaba.fastjson2.JSON;
import com.mybit.matching.core.disruptor.GroupDisruptorManager;
import com.mybit.matching.core.entity.OrderResult;
import com.mybit.matching.core.kafka.KafkaConsumer;
import com.mybit.matching.core.processor.KafkaDataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class KafkaConsumerTest {

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        /**
         * 开启自动检查时，需要使用一个symbol，因为架构设计是symbol并发执行的，symbol间offset不保证完全有序是正常的。
         */

        String topic = "orderResults05";
        new KafkaConsumerTest().startConsume(topic, false);
    }

    public void startConsume(String topic, boolean autoDiff) throws InterruptedException {

        GroupDisruptorManager symbolManager = GroupDisruptorManager.getInstance();

        AtomicLong received = new AtomicLong(0);
        AtomicLong count = new AtomicLong(0);

        KafkaConsumer kafkaConsumer = new KafkaConsumer("localhost:9094", "consumerTest", true, "earliest");
        kafkaConsumer.setProcessor(new KafkaDataProcessor(){

            AtomicLong totalTime = new AtomicLong(0);
            AtomicLong totalRecords = new AtomicLong(0);
            AtomicLong totalCount = new AtomicLong(0);

            private long errors = 0;
            private long lastOffset = -1;
            private long maxOffset = -1;
            private boolean hasData = false;

            Set<Long> noDiscarnoDdOffsets = new ConcurrentSkipListSet<>();

            /****** 多symbols ********/
            // 保证每个symbol内offset是有序递增的（非连续）
            // 由于不同symbol消费时间有差异，不能直接判断是否discard
            Collection<Long> noFoundOffsets = new HashSet<>();
            Collection<Long> counsumedOffsets = new HashSet<>();

            class SymbolContext {
                public long lastOffset = -1;
            }
            Map<String, SymbolContext> symbolMap = new HashMap<>();
            /**************/


            @Override
            public boolean onData(TopicPartition tp, Collection<ConsumerRecord<String, String>> records, long startTime) {

                if (records == null) {
                    // 去除已消费，去除主动丢弃
                    boolean b = noFoundOffsets.removeAll(counsumedOffsets);
                    List<Long> list = noFoundOffsets.stream().filter(offset -> !symbolManager.isDiscard("", offset)).toList();
//                    log.info("maxOffset: {}", maxOffset);
//                    log.info("noFoundOffsets: {}", list.size());
//                    log.info("noFoundOffsets: {}", list);
                    if (list.isEmpty()) counsumedOffsets.clear();

                    // 统计延迟
                    //SimpleMetric.printQps("result consumer qps", totalTime, totalRecords, totalCount, System.nanoTime(), 0);

                    long total = totalTime.get();
                    long count = totalCount.get();
                    if (count > 0 && count % 1000_000 == 0) {
                        log.info("AA result consumer qps: {}/{}={} {}", total, count, 1000_000_000L / (total / count), 0);
                    }

                    return true;
                }

                if (records.isEmpty()) {
                    return false;
                }

                if (!hasData) {
                    hasData = true;
                    log.info("开始处理数据，如果有问题会打印错误日志: \"检查失败\"");
                }
//                return processOneSymbol(tp, records);
                return processMultiSymbol(tp, records);
            }

            private boolean processMultiSymbol(TopicPartition tp, Collection<ConsumerRecord<String, String>> records) {
                received.addAndGet(records.size());
                long l = count.incrementAndGet();
                if (l % 10000 == 0) {
                    log.info("consumer test recv {} {}", count, received.get());
                }

                for (ConsumerRecord<String, String> record : records) {
                    // 多symbol时，每个symbol的offset只符合单调递增
                    List<OrderResult> list = JSON.parseArray(record.value(), OrderResult.class);
                    for (OrderResult item : list) {
                        long offset = item.getOffset();
                        String symbol = item.getGroupKey();

                        maxOffset = Math.max(maxOffset, offset);

                        SymbolContext ctx = symbolMap.computeIfAbsent(symbol, s -> new SymbolContext());

                        if (ctx.lastOffset < 0)
                            ctx.lastOffset = offset-1;
                        else if (ctx.lastOffset >= offset) {
                            log.error("检查失败：ctx.lastOffset >= offset = {}>={}", ctx.lastOffset, offset);
                        } else if (ctx.lastOffset+1 < offset) {
                            for (long i = ctx.lastOffset+1; i < offset; i++) {
                                if (symbolManager.isDiscard(symbol, i)) {
                                    //log.info("offset不连续，主动丢弃 {} {} {} {}", symbol, i, preOffset, offset);
                                    continue;
                                }
                                noFoundOffsets.add(i);      // 将缺失的暂时放入列表
                            }
                        }

                        noFoundOffsets.remove(offset);  // 如果存在，将消费过的删除
                        counsumedOffsets.add(offset);

                        ctx.lastOffset = offset;

                        // 统计延迟
                        //SimpleMetric.printQps("result consumer qps", totalTime, totalRecords, totalCount, event.getCreateTime(), 1);

                        long delta = System.nanoTime() - item.getCreateTime();
                        long total = totalTime.addAndGet(delta);
                        long count = totalCount.incrementAndGet();
                        if (count % 1000_000 == 0) {
                            log.info("AA result consumer qps: {}/{}={} {}", total, count, 1000_000_000L / (total / count), delta / 1000000L);
                        }
                    }
                }

                return true;
            }



            private boolean processOneSymbol(TopicPartition tp, Collection<ConsumerRecord<String, String>> records) {

                if (records.isEmpty()) {
                    return false;
                }

                if (!hasData) {
                    hasData = true;
                    log.info("开始处理数据，如果有问题会打印错误日志: \"检查失败\"");
                }

                received.addAndGet(records.size());
                long l = count.incrementAndGet();
                if (l % 10000 == 0) {
                    log.info("recv {} {}", count, received.get());
                }

                for (ConsumerRecord<String, String> record : records) {

                    long minOffset = -1, maxOffset = -1, preOffset = -1;
                    List<OrderResult> list = JSON.parseArray(record.value(), OrderResult.class);
                    for (OrderResult item : list) {
                        long offset = item.getOffset();
                        String symbol = item.getGroupKey();

//                        if (offset < 248636576) continue;

                        if (lastOffset < 0) lastOffset = offset-1;

                        if (minOffset < 0) {
                            minOffset = offset;
                        } else if (offset <= minOffset) {
                            errors++;
                            log.error("检查失败：批次内顺序错误1 {} {} {} {}", errors, minOffset, offset, record.offset());
                        }

                        maxOffset = Math.max(maxOffset, offset);

                        if (preOffset > 0 && preOffset +1 != offset) {
                            errors++;
                            if (autoDiff) {     // 发送位置主动丢弃一定比例的offset，并记录，这里自动进行比对【主动丢弃】
                                for (long i = preOffset + 1; i < offset; i++) {
                                    if (symbolManager.isDiscard(symbol, i)) {
                                        //log.info("offset不连续，主动丢弃 {} {} {} {}", symbol, i, preOffset, offset);
                                    } else {
                                        log.error("检查失败：offset不连续，被动丢弃 {} {} {} {}", symbol, i, preOffset, offset);
                                        if (noDiscarnoDdOffsets.contains(offset)) {
                                            log.error("检查失败：已经消费过了 {}", offset);
                                        }
                                    }
                                }
                            } else {
                                log.error("检查失败：offset不连续 {} {}", preOffset, offset);
                            }
                        }

                        noDiscarnoDdOffsets.add(offset);


                        preOffset = offset;
                    }

                    if (minOffset < 0) continue;
                    if (minOffset >= maxOffset && list.size() != 1) {
                        ++errors;
                        log.error("检查失败：批次内顺序有误 {} {} {} {} {}", errors, minOffset, maxOffset, records.size(), record.offset());
                    }

                    if (minOffset != lastOffset + 1) {
                        ++errors;
                        if (autoDiff) {     // 发送位置主动丢弃一定比例的offset，并记录，这里自动进行比对【主动丢弃】
                            for (long i = lastOffset + 1; i < minOffset; i++) {
                                if (symbolManager.isDiscard("", i)) {
                                    //log.info("offset不连续，主动丢弃 {} {} {} {}", symbol, i, preOffset, offset);
                                } else {
                                    log.error("检查失败：offset不连续 {} {} {} {}", "symbol", i, lastOffset, minOffset);
                                }
                            }
                        } else {
                            log.error("检查失败：minOffset != lastOffset+1        errors={} lastOffset={} minOffset={} {}", errors, lastOffset, minOffset, record.offset());
                        }
                    }
                    // 多交易对同时测试时，这个不一定成立
                    if (minOffset <= lastOffset) {
                        ++errors;
                        log.error("检查失败：本批次内offset小于上批次最大offset errors={} lastOffset={} minOffset={} {}", errors, lastOffset, minOffset, record.offset());
                    }

                    lastOffset = maxOffset;
                }

                return true;
            }
        });


        TopicPartition tp = new TopicPartition(topic, 0);
        Map<TopicPartition, Long> partitionOffsetMap = new HashMap<>();
//        partitionOffsetMap.put(tp, 3350902L);
        kafkaConsumer.setStartConsumeTopicPartitionOffsets(Collections.singleton(tp), partitionOffsetMap);  // 设置起始消费位置

        kafkaConsumer.setInterval(5L);
        kafkaConsumer.startup();

        Thread.sleep(300_000);
    }

}
