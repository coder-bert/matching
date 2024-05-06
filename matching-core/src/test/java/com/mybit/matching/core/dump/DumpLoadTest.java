package com.mybit.matching.core.dump;

import com.alibaba.fastjson2.JSON;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.mybit.matching.core.orderbook.FileNameRecord;
import com.mybit.matching.core.utils.FileTraversal;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.*;


@Slf4j
public class DumpLoadTest {

    @Test
    public void testOrderBookDumpLoad() {

        String dumpPath = "C:\\tmp\\matching";
        Collection<String> orderBookDumpFiles = FileTraversal.enumFiles(dumpPath, null, "orderbook", ".log");

//        TreeMap<Long, FileNameRecord> sortedMap = Maps.newTreeMap();
        TreeMap<Long, List<FileNameRecord>> sortedMap = Maps.newTreeMap();
        orderBookDumpFiles.forEach(fileName -> {

            // format: order@timestamp@topic@partition@offset1@offset2@dstTopic@dstP@offset@0.dump.log
            List<String> lst = Splitter.on("+").splitToList(fileName);

            if (lst.size() == 11) {
                FileNameRecord fileNameRecord = FileNameRecord.builder()
                        .fileName(fileName)
                        .timestamp(Long.parseLong(lst.get(1)))
                        .topic(lst.get(2))
                        .partition(Integer.parseInt(lst.get(3)))
                        .matchedOffset(Long.parseLong(lst.get(4)))
                        .sentOffset(Long.parseLong(lst.get(5)))
                        .dstTopic(lst.get(6))
                        .dstPartition(Integer.parseInt(lst.get(7)))
                        .dstSentOffset(Long.parseLong(lst.get(8)))
                        .build();

                List<FileNameRecord> fileNameRecords = sortedMap.computeIfAbsent(fileNameRecord.getTimestamp(), t -> new ArrayList<>());
                fileNameRecords.add(fileNameRecord);
                //sortedMap.put(fileNameRecord.getTimestamp(), fileNameRecord);
            }

            // 同步发送模式下：offset1和offset2应该是一致的  todo 不一致情况下如何处理？--记录发送后未确认记录，一同dump
            //

        });

        NavigableSet<Long> descKeys = sortedMap.descendingKeySet();

        log.info("{}", JSON.toJSONString(sortedMap));

        Map.Entry<Long, List<FileNameRecord>> firstEntry = sortedMap.firstEntry();
        Map.Entry<Long, List<FileNameRecord>> lastEntry = sortedMap.lastEntry();
        log.info("{} {}", firstEntry.getKey(), firstEntry.getValue());
        log.info("{} {}", lastEntry.getKey(), lastEntry.getValue());
    }
}
