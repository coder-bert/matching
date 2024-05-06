package com.mybit.matching.core.orderbook;

import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.dump.Dump;
import com.mybit.matching.core.entity.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


@Data
@Slf4j
public class OrderBookImpl implements OrderBook {

    private String groupKey;
    private String symbol;

    private List<OrderEvent> buy = new ArrayList<>();        // todo
    private List<OrderEvent> sell = new ArrayList<>();       // todo

    private List<Event> testQueues = new ArrayList<>(10_000);

    public OrderBookImpl(String groupKey, String symbol) {
        this.groupKey = groupKey;
        this.symbol = symbol;
    }

    @Override
    public String getSymbol() {
        return this.symbol;
    }

    @Override
    public OrderResult match(Event order) {
        return innerMatch(order);
    }


    private OrderResult innerMatch(Event event) {

        OrderResult orderResult = new OrderResult(0, "todo", event);

        orderResult.setTopic(event.getTopic());
        orderResult.setPartition(event.getPartition());
        orderResult.setOffset(event.getOffset());
        orderResult.setKey(event.getKey());
        orderResult.setHeaders(event.getHeaders());

        orderResult.setGroupKey(event.getGroupKey());
        orderResult.setSeq(event.getSeq());
        orderResult.setCreateTime(event.getStartTime());

        // todo matching

        if (Config.getInstance().isTest() && testQueues.size() < 100_0000) {
            testQueues.add(event);
        }

        return orderResult;
    }

    @Override
    public void dump(DumpEvent event, FileNameRecord fileNameRecord) {
        String dumpPath = event.getDumpPath();
        long fileTimeStamp = event.getTimestamp();
        boolean isClose = event instanceof CloseDumpEvent;

        long startTime = System.currentTimeMillis();

        if (fileNameRecord.matchedOffset != fileNameRecord.sentOffset) {
            log.error("撮合的offset和已发送offset不一致 {} {} {} {}", fileNameRecord.topic, fileNameRecord.partition, fileNameRecord.matchedOffset, fileNameRecord.sentOffset);
        }

        log.info("orderBook dump start {} {} @{} {}", groupKey, symbol, System.identityHashCode(event), testQueues.size());
        // prefix/orderbook+timestamp+topic+partition+offset1+offset2+topic+partition+offset3+symbol+0|1.log
        String dumpOrderBookFile = String.format("%s/orderbook+%d+%s+%d+%d+%d+%s+%d+%d+%s+%d.log", dumpPath, fileTimeStamp,
                fileNameRecord.topic, fileNameRecord.partition,
                fileNameRecord.matchedOffset, fileNameRecord.sentOffset,
                fileNameRecord.dstTopic, fileNameRecord.dstPartition, fileNameRecord.dstSentOffset,
                symbol, isClose ? 1 : 0);
        boolean res = Dump.writeOrderBookByFileChannel(dumpOrderBookFile, testQueues);
        if (res) {
            log.info("orderBook dump finished {} {} @{} {} {} {}ms", groupKey, symbol, System.identityHashCode(event), dumpOrderBookFile, testQueues.size(), System.currentTimeMillis() - startTime);
        } else {
            log.error("orderBook dump failed {} {} @{} {} {}", groupKey, symbol, System.identityHashCode(event), dumpOrderBookFile, System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public void addEvent(OrderEvent event) {
        // todo 加载到内存
        //log.info("OrderBook addEvent");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderBookImpl orderBook = (OrderBookImpl) o;
        return Objects.equals(symbol, orderBook.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol);
    }
}
