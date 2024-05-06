package com.mybit.matching.core.orderbook;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderBookFactory {

    public static OrderBook newOrderBook(String groupKey, String symbol) {      // todo SPI + 反射
        log.info("new orderBook {} {}", groupKey, symbol);
        return new OrderBookImpl(groupKey, symbol);
    }
}
