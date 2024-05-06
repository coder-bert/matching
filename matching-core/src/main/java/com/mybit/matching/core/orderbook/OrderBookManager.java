package com.mybit.matching.core.orderbook;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mybit.matching.core.InitializeAndCloseable;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.disruptor.GroupDisruptor;
import com.mybit.matching.core.dump.Dump;
import com.mybit.matching.core.entity.LoadEvent;
import com.mybit.matching.core.exception.BaseException;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.FileTraversal;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


@Slf4j
@Data
public class OrderBookManager implements InitializeAndCloseable {

    private final Config config = Config.getInstance();

    private boolean isRunning = false;

    private final Map<String, OrderBook> orderBooks = new ConcurrentHashMap<>(128);

    // 撮合进度
    private Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap = new ConcurrentHashMap<>(128);


    private OrderBookManager() {
    }

    private static volatile OrderBookManager instance;

    public static OrderBookManager getInstance() {
        if (instance == null) {
            synchronized (OrderBookManager.class) {
                if (instance == null) {
                    instance = new OrderBookManager();
                }
            }
        }
        return instance;
    }

    @Override
    public boolean init() {
        // 加载dump放到Dump.loadLastestDump函数中
        return true;
    }

    private void searchStartupOffsetAndLoad(String dumpPath,
                                            Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap,
                                            Map<String, String> orderBookDumpFiles) throws IOException {
        // 获取最新文件（文件名时间戳）
        // 可以手动编辑或者新建最新的文件，采用自动加载流程

        Collection<String> offsetFiles = FileTraversal.enumFiles(dumpPath, null, "offset-", ".log");
        TreeMap<Long, String> map = new TreeMap<>();      // 有序
        offsetFiles.forEach(fileName -> {
            List<String> lst = Splitter.on("-").splitToList(fileName);
            AssertUtil.isTrue(lst.size() >= 3, "offset文件名格式：offset-ts-X.log");

            Long ts = Long.parseLong(lst.get(1));
            map.put(ts, fileName);
        });


        String latestOffsetDumpFile = null;
        if (!map.isEmpty()) {    // 未找到offset文件
            Map.Entry<Long, String> entry = map.lastEntry();        // 最新文件
            Long lastestTimestamp = entry.getKey();
            latestOffsetDumpFile = entry.getValue();
            log.info("最新offset文件：{}  时间戳：{}", latestOffsetDumpFile, lastestTimestamp);

            boolean res = Dump.readOffsetsBufferedReader(latestOffsetDumpFile, topicPartitionOffsetMap, orderBookDumpFiles);
            if (!res) {
                throw new BaseException("加载最新offset文件失败，请确认是否有效 " + dumpPath);
            }
        }
    }

    @Override
    public boolean startup() {
        if (isRunning) return false;
        isRunning = true;
        return true;
    }

    @Override
    public boolean flush() {
        return true;
    }

    @Override
    public boolean close() {
        if (!isRunning)
            return false;

        isRunning = false;
        return true;
    }

    /**
     * 添加交易对
     *
     * @param symbol 必须非空
     * @return true|false
     */
    public OrderBook getOrderBookOrCreate(String groupKey, String symbol) {
        AssertUtil.notEmpty(groupKey, "invalid groupKey");
        AssertUtil.notEmpty(symbol, "invalid symbol");

        return orderBooks.computeIfAbsent(symbol, k -> OrderBookFactory.newOrderBook(groupKey, symbol));
    }

    public int getOrderBookSize() {
        return orderBooks.size();
    }
    public Set<String> getSymbols() {
        return orderBooks.keySet();
    }


    /**
     * kafka consumer起始消费的位置
     *
     * @return
     */
    public Map<TopicPartition, Long> getPartitionOffsetMap() {
        return topicPartitionOffsetMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getToConsumingOffset()));
    }

    /**
     * kafka consumer订阅topics列表
     *
     * @return
     */
    public Collection<TopicPartition> getTopicPartitions() {
        return topicPartitionOffsetMap.keySet();
    }

    /**
     * 启动时调用，加载开始消费位置
     *
     * @param topicPartitionOffsets
     */
    public void setTopicPartitionOffsets(Collection<TopicPartitionOffset> topicPartitionOffsets) {
        if (topicPartitionOffsets != null) {
            topicPartitionOffsets.forEach(item -> {
                topicPartitionOffsetMap.put(item.getSrcTp(), item);
            });
        }
    }

    /**
     * 撮合完成后调用，记录当前已完成撮合的offset
     *
     * @param srcTp
     * @param offset
     */
    synchronized public void updateSuccessMatchingOffset(TopicPartition srcTp, Long offset) {
        //log.info("撮合进度 {} {}", srcTp, offset);
        TopicPartitionOffset topicPartitionOffset = topicPartitionOffsetMap.computeIfAbsent(srcTp, t -> new TopicPartitionOffset());
        topicPartitionOffset.setSrcTp(srcTp);
        topicPartitionOffset.setSrcMatchedOffset(offset);
    }

    /**
     * 撮合完成, 并且向下游kafka发送成功后调用，更新成功发送的位置记录
     * <p>
     * NOTE: 撮合完成和发送完成可能存在一定的差距
     *
     * @param srcTp     源kafka的topic + partition
     * @param srcOffset 发送成功记录的源kafka的offset
     * @param dstTp     下游kafka的topic + partition
     * @param dstOffset 发送成功记录的下游kafka的offset
     */
    synchronized public void updateSuccessSentOffset(TopicPartition srcTp, Long srcOffset, TopicPartition dstTp, long dstOffset) {
        //log.info("发送进度 {} {} {} {}", srcTp, srcOffset, dstTp, dstOffset);
        TopicPartitionOffset topicPartitionOffset = topicPartitionOffsetMap.computeIfAbsent(srcTp, t -> new TopicPartitionOffset());
        topicPartitionOffset.setSrcTp(srcTp);
        topicPartitionOffset.setSrcSentOffset(srcOffset);
        topicPartitionOffset.setDstTp(dstTp);
        topicPartitionOffset.setDstSentOffset(dstOffset);
    }

    synchronized public FileNameRecord getSentOffset(TopicPartition tp) {
        TopicPartitionOffset o = topicPartitionOffsetMap.get(tp);
        if (o == null) {
            return null;
        }
        return FileNameRecord.builder()
                .timestamp(o.getUpdateTime())
                .topic(o.getSrcTp().topic())
                .partition(o.getSrcTp().partition())
                .matchedOffset(o.getSrcMatchedOffset())
                .sentOffset(o.getSrcSentOffset())
                .dstTopic(o.getDstTp().topic())
                .dstPartition(o.getDstTp().partition())
                .dstSentOffset(o.getDstSentOffset())
                .num(topicPartitionOffsetMap.size())
                .build();
    }

    synchronized public Map<TopicPartition, TopicPartitionOffset> copyTopicPartitionOffsetMap() {
        return new HashMap<>(topicPartitionOffsetMap);
    }

}
