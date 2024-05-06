package com.mybit.matching.core.orderbook;

import com.mybit.matching.core.entity.DumpEvent;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.OrderEvent;
import com.mybit.matching.core.entity.OrderResult;

public interface OrderBook {

    String getSymbol();

    /**
     * 进行撮合交易
     * @param order
     * @return
     */
    OrderResult match(Event order);


    /**
     * 生成dump文件
     * @param event
     * @param fileNameRecord
     */
    void dump(DumpEvent event, FileNameRecord fileNameRecord);


    /**
     * 从dump文件加载时添加进orderBook
     *
     * @param event
     */
    void addEvent(OrderEvent event);
}