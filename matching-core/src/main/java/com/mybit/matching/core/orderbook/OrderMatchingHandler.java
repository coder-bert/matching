package com.mybit.matching.core.orderbook;

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Sets;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.disruptor.DataEvent;
import com.mybit.matching.core.disruptor.GroupDisruptorManager;
import com.mybit.matching.core.dump.Dump;
import com.mybit.matching.core.entity.*;
import com.mybit.matching.core.sender.Sender;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.CollectionUtil;
import com.mybit.matching.core.utils.SimpleMetric;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


@Slf4j
public class OrderMatchingHandler implements BaseEventHandler<Event> {

    private final Config config = Config.getInstance();
    private final OrderBookManager orderBookManager = OrderBookManager.getInstance();
    private final GroupDisruptorManager groupDisruptorManager = GroupDisruptorManager.getInstance();
    private final Set<OrderBook> orderBooks = Sets.newHashSet();

    private String groupKey;
    private TopicPartition tp;
    private Sender sender;                      //  撮合后下游发送

    private boolean isRunning = false;           // 关闭后不能再处理

    private Collection<Event> orderEventsCache = new ArrayList<>(1024 * 2);     // 批处理

    private DumpEvent lastDumpEvent = null;    // 上次dump事件，避免重复

    private static AtomicLong totalTime = new AtomicLong(0);
    private static AtomicLong totalCount = new AtomicLong(0);
    private static AtomicLong totalRecords = new AtomicLong(0);

    private static AtomicLong totalSentTime = new AtomicLong(0);
    private static AtomicLong totalSentCount = new AtomicLong(0);
    private static AtomicLong totalSentRecords = new AtomicLong(0);

    // test only 记录丢弃的offset，用于自动化比较
    private ConcurrentSkipListSet<Long> discardOffsets = new ConcurrentSkipListSet<>();       // 测试用，记录丢弃的offset，跟下游比对测试
    private long firstOffset = -1;

    public OrderMatchingHandler(String groupKey, TopicPartition tp, Sender sender) {
        this.groupKey = groupKey;
        this.tp = tp;
        this.sender = sender;
    }

    @Override
    public boolean init() {
        sender.init();
        return true;
    }

    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;
        sender.startup();
        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning) return false;
        log.info("handler close {}", groupKey);
        isRunning = false;
        sender.close();
        return true;
    }


    /**
     *   disruptor框架事件触发
     *
     * Called when a publisher has published an dataEve to the {@link RingBuffer}.  The {@link BatchEventProcessor} will
     * read messages from the {@link RingBuffer} in batches, where a batch is all of the events available to be
     * processed without having to wait for any new dataEve to arrive.  This can be useful for dataEve handlers that need
     * to do slower operations like I/O as they can group together the data from multiple events into a single
     * operation.  Implementations should ensure that the operation is always performed when endOfBatch is true as
     * the time between that message and the next one is indeterminate.
     *
     * @param dataEvent  published to the {@link RingBuffer}
     * @param sequence   of the dataEve being processed
     * @param endOfBatch flag to indicate if this is the last dataEve in a batch from the {@link RingBuffer}
     * @throws Exception if the EventHandler would like the exception handled further up the chain.
     */
    @Override
    public void onEvent(DataEvent dataEvent, long sequence, boolean endOfBatch) throws Exception {
        if (!isRunning) return;

        long startTime = dataEvent.getStartTime();
        String groupKey = dataEvent.getGroupKey();
        Collection<Event> events = dataEvent.getEvents();
        if (events.isEmpty()) return;


        // 控制事件时只有一个event
        Event firstEvent = events.iterator().next();
        if (!(firstEvent instanceof OrderEvent)) {
            if (firstEvent instanceof RegularDumpEvent || firstEvent instanceof CloseDumpEvent) {
                dump((DumpEvent) firstEvent);
            } else if (firstEvent instanceof LoadEvent loadEvent) {
                log.info("LoadEvent {} @{} {}", groupKey, System.identityHashCode(loadEvent), loadEvent.getStartTime());
                load(loadEvent);
            }
            return;
        }

        // 查询是否有CloseDumpEvent，避免在消息堆积情况下延迟比较大，影响退出
        DumpEvent dumpEvent = groupDisruptorManager.hasCloseDumpEvent();
        if (dumpEvent != null && dumpEvent != lastDumpEvent) {
            dump(dumpEvent);
            return;
        }

        // 委托单必须是OrderEvent类及其子类

        if (config.isBatch()) {
            orderEventsCache.addAll(events);
            if (!endOfBatch && orderEventsCache.size() < config.getBatchSize()) {
                return;
            }
            events = orderEventsCache;
        }

        int size = events.size();
        onEvent(groupKey, events, startTime);
        orderEventsCache.clear();   // batch模式时发送成功后清空，TODO 如果一直不清空可能会超过最大发送大小
    }


    /**
     * 自定义业务处理逻辑：撮合
     *
     * @param groupKey
     * @param orderEvents
     * @param startTime
     * @return
     */
    @Override
    public boolean onEvent(String groupKey, Collection<Event> orderEvents, long startTime) {
        try {
            if (config.isBatch()) {
                return matchBatch(groupKey, orderEvents, startTime);
            } else {
                return matchOnce(groupKey, orderEvents, startTime);
            }
        } catch (Exception e) {
            log.error("matching failed {}", JSON.toJSONString(orderEvents), e);
        }
        return false;
    }

    public boolean matchOnce(String groupKey, Collection<Event> orderEvents, long startTime) {
        if (CollectionUtil.isEmpty(orderEvents)) return false;

        orderEvents.forEach(orderEvent -> {
            String symbol = orderEvent.getSymbol();
            AssertUtil.notEmpty(symbol, "记录没有设置交易对");

            OrderBook orderBook = orderBookManager.getOrderBookOrCreate(groupKey, symbol);
            orderBooks.add(orderBook);
            OrderResult matchResult = orderBook.match(orderEvent);  // 触发撮合
            if (OrderResult.isOK(matchResult)) {
                // 撮合成功时，更新撮合进度（offset）
                orderBookManager.updateSuccessMatchingOffset(new TopicPartition(orderEvent.getTopic(), orderEvent.getPartition()), orderEvent.getOffset());
            }

            SimpleMetric.printQps("matching qps", totalTime, totalRecords, totalCount, startTime, 1);

            long sendOffset = sender.sendOne(orderEvent, matchResult);
            if (sendOffset < 0) {
                log.error("TODO kafka send failed. How to deal with it？ {} {} {} {}", orderEvent.getOffset(), orderEvent.getPartition(), orderEvent.getOffset(), JSON.toJSONString(orderEvent));
            }
        });

        return true;
    }

    public boolean matchBatch(String groupKey, Collection<Event> events, long startTime) {
        if (CollectionUtil.isEmpty(events)) return false;

        // NOTE: Consumer中消费到的记录已经根据TopicPartition分组，这里的数据都是一个Topic+Partition下的

        String srcKafkaTopic = "";
        int srcKafkaPartition = -1;
        long maxOffsetInBatch = -1L;
        String logid = UUID.randomUUID().toString();

        List<OrderResult> matchResults = new ArrayList<>(events.size());
        for (Event event : events) {
            srcKafkaTopic = event.getTopic();
            srcKafkaPartition = event.getPartition();
            maxOffsetInBatch = Math.max(maxOffsetInBatch, event.getOffset());

            //if (config.isTest()) {
            if (firstOffset < 0) {
                firstOffset = event.getOffset();
                log.info("the first offset matching {} {}", groupKey, firstOffset);
            }
            //}

            // test only 随机丢弃offset
            if (config.isTestDiscard() && ThreadLocalRandom.current().nextInt(10) <= 1) {
                //log.info("server丢弃 {} {}", symbol, event.getOffset());
                discardOffsets.add(event.getOffset());
                discardOffsets.removeIf(offset -> offset < event.getOffset() - 100_000_000);   // 保留最近N个
                continue;
            }

            String symbol = event.getSymbol();
            AssertUtil.notEmpty(symbol, "symbol not found");

            OrderBook orderBook = orderBookManager.getOrderBookOrCreate(groupKey, symbol);    // 交易对对应的订单簿
            orderBooks.add(orderBook);
            OrderResult matchResult = orderBook.match(event);  // 撮合
            matchResults.add(matchResult);
        }

        // test only, 由于全部丢弃不往后续发送，正式跑必须每条order都有结果
        if (config.isTest() && matchResults.isEmpty()) {
            return true;
        }

        SimpleMetric.printQps("matching qps", totalTime, totalRecords, totalCount, startTime, matchResults.size());

        // 更新撮合进度
        orderBookManager.updateSuccessMatchingOffset(new TopicPartition(srcKafkaTopic, srcKafkaPartition), maxOffsetInBatch);

        if (config.isSendAsync()) {
            // 异步发送，经过retries或者max.block.ms时间后失败，后续消息有可能成功，导致部分数据顺序错误
            long finalMaxOffsetInBatch = maxOffsetInBatch;
            sender.sendBatch(logid, matchResults, srcKafkaTopic, srcKafkaPartition, maxOffsetInBatch, 0, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        log.error("kafka send failed. {} {} {} {}", logid, recordMetadata.topic(), recordMetadata.partition(), finalMaxOffsetInBatch, e);
                    } else {
                        long sendOffset = recordMetadata != null ? recordMetadata.offset() : -1;
                        if (sendOffset >= 0) {
                            SimpleMetric.printQps("kafka send qps", totalSentTime, totalSentRecords, totalSentCount, startTime, matchResults.size());
                        } else {
                            log.error("kafka发送成功，但是offset=-1，可能配置了acks=0 {} {}", logid, recordMetadata.topic());
                        }
                    }
                }
            });
            return true;
        } else {
            // 同步发送
            long sendOffset = sender.sendBatch(groupKey, matchResults, srcKafkaTopic, srcKafkaPartition, maxOffsetInBatch);
            if (sendOffset >= 0) {
                SimpleMetric.printQps("kafka send qps", totalSentTime, totalSentRecords, totalSentCount, startTime, matchResults.size());
                return true;
            }
            log.error("kafka send failed {} {} {} {}", groupKey, srcKafkaTopic, srcKafkaPartition, maxOffsetInBatch);
        }
        return false;
    }

    private void dump(DumpEvent dumpEvent) {
        if (dumpEvent == lastDumpEvent) {
            log.info("DumpEvent已经处理 {} @{} @{} ", groupKey, System.identityHashCode(dumpEvent), System.identityHashCode(lastDumpEvent));
            return;     // 避免重复
        }
        lastDumpEvent = dumpEvent;

        long startTime = System.currentTimeMillis();

        // 此函数是多线程调用，有多少撮合线程就有几个
        // 每个撮合线程顺序执行撮合和dump，非并发
        // DUMP OFFSETS时，需要所有撮合线程同步

        // 1. sender.flush  触发立即发送数据，尽量多的发送完成
        // 2. orderbook dump  这是已经停止撮合了, orderbook数据保持不变, 可以持久化完成
        // 3. CountDownLatch同步, dump消费/撮合/发送进度信息
        // 4. await消费进度保存完成
        // 5. notifyAll触发开始继续消费[regularDump]

        boolean isClose = false;
        if (dumpEvent instanceof RegularDumpEvent) {
            log.info("RegularDumpEvent {} @{} {}", groupKey, System.identityHashCode(dumpEvent), dumpEvent.getStartTime());
        } else if (dumpEvent instanceof CloseDumpEvent) {
            isClose = true;
            log.info("CloseDumpEvent {} @{} {}", groupKey, System.identityHashCode(dumpEvent), dumpEvent.getStartTime());
            isRunning = false;          // 不再处理，因为sender会关闭，避免之后再处理数据抛出异常
            sender.close();
        }

        FileNameRecord fileNameRecord = orderBookManager.getSentOffset(tp);
        log.info("orderbooks will dump symbols this thread {} {} {}", groupKey, orderBooks.stream().map(OrderBook::getSymbol).collect(Collectors.toList()), fileNameRecord);
        orderBooks.forEach(orderBook -> dumpOrderBookFile(tp, fileNameRecord, dumpEvent, orderBook));
        dumpEvent.getOrderBookDumpLatch().countDown();

        if (isClose) {
            // 抢占leader：如果是关闭事件时，由leader等待所有dump完成，然后关闭closeLatch，进而Engine退出
            if (dumpEvent.getOffsetDumpLock().compareAndSet(false, true)) {
                if (dumpEvent.getCount() > 1) {     // 只有一个并发线程时不需要同步
                    log.info("[main] waiting for all orderBook dump to finish {}", groupKey);
                    try {
                        dumpEvent.getOrderBookDumpLatch().await();   // 等待所有orderbook dump完成
                        log.info("[main] waiting for all orderBook dump finished {}", groupKey);
                    } catch (InterruptedException e) {
                        //throw new RuntimeException(e);
                        log.error("[main] waiting for all orderBook dump exception {}", groupKey, e);
                    }
                }

                ((CloseDumpEvent) dumpEvent).getCloseLatch().countDown();
            }
        }

        log.info("dump finished {} @{} {} elapse: {}", groupKey, System.identityHashCode(dumpEvent), dumpEvent.getTimestamp(), System.currentTimeMillis() - startTime);
    }

    /**
     * orderbook持久化调用（可能是多线程调用）
     *
     * @param dumpEvent
     * @param orderBook
     */
    private void dumpOrderBookFile(TopicPartition tp, FileNameRecord fileNameRecord, DumpEvent dumpEvent, OrderBook orderBook) {
        orderBook.dump(dumpEvent, fileNameRecord);
    }

//    private void dumpOffsetFile(String groupKey, Sender sender, FileNameRecord fileNameRecord, DumpEvent dumpEvent, boolean isClose) {
//        String dumpPath = dumpEvent.getDumpPath();
//        long fileTimeStamp = dumpEvent.getTimestamp();
//        if (dumpEvent.getOffsetDumpLock().compareAndSet(false, true)) {
//            // 由自己 DUMP dump消费/撮合/发送进度信息
//            String dumpOffsetFile = String.format("%s/offset-%d-%d.log", dumpPath, fileTimeStamp / 86400_000L, 0);
//            try {
//                if (dumpEvent.getCount() > 1) {     // 只有一个并发线程时不需要同步
//                    log.info("[main] waiting for all orderBook dump to finish {}", groupKey);
//                    dumpEvent.getOrderBookDumpLatch().await();   // 等待所有orderbook dump完成
//                }
//                if (isClose) {
//                    sender.close();             // 关闭发送
//                    Thread.sleep(10);     // 稍等10ms，尽量保证已撮合数据发送成功
//                } else {
//                    //sender.flush();           // kafkaProducer.flush, 耗时≈200ms
//                }
//
//                // dump消费/撮合/发送进度信息
////                final Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap = orderBookManager.copyTopicPartitionOffsetMap(); // copy on write
////                Dump.writeOffsetsBufferedWriter(dumpOffsetFile, fileTimeStamp, topicPartitionOffsetMap, dumpEvent.getSymbolDumpFiles());
//                log.info("[main] offset dump finished {} @{} {} {}", groupKey, System.identityHashCode(dumpEvent), fileTimeStamp, dumpOffsetFile);
//            } catch (InterruptedException e) {
//                log.error("[main] offset dump failed {} @{} {} {}", groupKey, System.identityHashCode(dumpEvent), fileTimeStamp, dumpOffsetFile, e);
//            } finally {
//                if (dumpEvent.getCount() > 1) {     // 只有一个并发线程时不需要同步
//                    synchronized (dumpEvent) {
//                        log.info("[main] contExecLock.notifyAll {} @{}", groupKey, System.identityHashCode(dumpEvent));
//                        dumpEvent.getContExecLock().compareAndSet(false, true);
//                        dumpEvent.notifyAll();    // 触发所有OrderBook继续
//                    }
//                    if (isClose) {
//                        ((CloseDumpEvent) dumpEvent).getCloseLatch().countDown();
//                    }
//                }
//            }
//        } else {
//            // 等待 dump消费/撮合/发送进度信息 完成
//            try {
//                log.info("[slave] waiting for dump contExecLock... {} @{}", groupKey, System.identityHashCode(dumpEvent));
//                long startLockTime = System.currentTimeMillis();
//                synchronized (dumpEvent) {
//                    while (!dumpEvent.getContExecLock().get()) {
//                        dumpEvent.wait(2);             // 阻塞等待线程同步
//                    }
//                }
//                log.info("[slave] waiting for dump contExecLock finish. {} @{} {}", groupKey, System.identityHashCode(dumpEvent), System.currentTimeMillis() - startLockTime);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//    }


    private void load(LoadEvent loadEvent) {
        long startTime = System.currentTimeMillis();
        List<FileNameRecord> fileNameRecords = loadEvent.getFileNameRecords();
        log.info("load orderBook dump start {} {} {}", groupKey, System.identityHashCode(loadEvent), fileNameRecords);
        if (fileNameRecords != null) {
            fileNameRecords.forEach(fileNameRecord -> {
                Dump.readOrderBookByFileChannel(fileNameRecord.fileName, this::addEvent);
            });
        }
        log.info("load orderBook dump finished {} {} {}ms", groupKey, System.identityHashCode(loadEvent), System.currentTimeMillis() - startTime);
    }

    private void addEvent(OrderEvent event) {
        String groupKey = event.getGroupKey();
        String symbol = event.getSymbol();

        OrderBook orderBook = orderBookManager.getOrderBookOrCreate(groupKey, symbol);
        orderBooks.add(orderBook);
        orderBook.addEvent(event);
    }



    /**
     * test only
     */
    public boolean isDiscard(String groupKey, long offset) {
        return discardOffsets.contains(offset);
    }

    public long firstDiscardOffset(String groupKey, long offset) {
        if (discardOffsets.isEmpty()) return -1;
        return discardOffsets.first();
    }
}
