package com.mybit.matching.core.dump;

import com.google.common.collect.Lists;
import com.mybit.matching.core.entity.Event;
import com.mybit.matching.core.entity.OrderEvent;
import com.mybit.matching.core.exception.OrderBookFileFormatException;
import com.mybit.matching.core.orderbook.TopicPartitionOffset;
import com.mybit.matching.core.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
public class DumpTest {


    private List<Event> testOrderEventQueue(int N) {
        List<Event> testQueues = new ArrayList<>(10_0000);

        //int N = 100;
        for (int i = 0; i < N; i++) {
            OrderEvent orderEvent = new OrderEvent();
            testQueues.add(orderEvent);
        }

        return testQueues;
    }


    //    @Test
    public void testDumpAndParse() throws IOException {

        Map<TopicPartition, TopicPartitionOffset> map = new ConcurrentHashMap<>();
        Map<TopicPartition, TopicPartitionOffset> map2 = new ConcurrentHashMap<>();

        Map<String, String> symbolDumpFles = new HashMap<>();
        Map<String, String> symbolDumpFles2 = new HashMap<>();

        //symbolDumpFles.put("ETH", "ETH_DumpFile");

        String data = String.format("%d", Dump.magic) + ",123,239631271580,2,3,topic05,0,29740706,29740706,orderResults05,0,8921756,topic06,1,29740707,29740707,orderResults06,1,8921757,ETH,ETH_DumpFile,ETH2,ETH_DumpFile2,ETH3,ETH_DumpFile3";
        Dump.parseOffsetsDump(data, map, symbolDumpFles);

        int len = 0;
        long now = 239631271580L;

        boolean hasError = false;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 100_0000; i++) {
            String data2 = Dump.writeOffsetsBufferedWriter(now, map, symbolDumpFles);     // dump
            assertEquals(data, data2);

            len = data2.length();
            boolean ret = Dump.parseOffsetsDump(data2, map2, symbolDumpFles2);     // parse
            if (!ret) {
                hasError = true;
            }
        }
        log.info("{} {} {}", hasError, len, System.currentTimeMillis() - startTime);
        assertEquals(map2, map);
        assertEquals(symbolDumpFles, symbolDumpFles2);
        assertFalse(hasError);

//        len = 0;
//        startTime = System.currentTimeMillis();
//        for (int i = 0; i < 100_0000; i++) {
//            ByteBuffer buffer = Dump.dumpOffsetsBuffer(map, now);    // dump
//            len = buffer.position();
//            boolean ret = Dump.parseOffsetsDump(buffer, map2);        // parse
//            if (!ret) {
//                hasError = true;
//            }
//        }
//        log.info("{} {} {}", hasError, len, System.currentTimeMillis() - startTime);
//        assertEquals(map2, map);
//        assertFalse(hasError);
    }

//    @Test
//    public void testDumpWrite() throws IOException {
//
//        Map<TopicPartition, TopicPartitionOffset> map = new ConcurrentHashMap<>();
//        Map<TopicPartition, TopicPartitionOffset> map2 = new ConcurrentHashMap<>();
//        Map<TopicPartition, TopicPartitionOffset> map3 = new ConcurrentHashMap<>();
//
//        Map<String, String> symbolDumpFles = new HashMap<>();
//
//
//        String data = String.format("%d", Dump.magic) + ",123,239631271580,2,3,topic05,0,29740706,29740706,orderResults05,0,8921756,topic06,1,29740707,29740707,orderResults06,1,8921757,ETH,ETH_DumpFile,ETH2,ETH_DumpFile2,ETH3,ETH_DumpFile3";
//        Dump.parseOffsetsDump(data, map, symbolDumpFles);
//
//        long now = 239631271580L;
//
//        long startTime = System.currentTimeMillis();
//
//        String offsetDumpFile = String.format("c:\\tmp\\offset-%s.log", now);
////        String offsetDumpFile = "c:\\tmp\\matching\\offset-19841-0.log";
//
//        ByteBuffer buffer = Dump.dumpOffsetsBuffer(map, now);    // dump
//        Dump.writeOffsetFileChannel(offsetDumpFile, buffer);
//
//        Optional<List<ByteBuffer>> buffer2Opt = Dump.readOffsetFileChannel(offsetDumpFile);
//        assertTrue(buffer2Opt.isPresent());
//        //assertEquals(buffer2Opt.get().size(), 1);
//        buffer2Opt.ifPresent(buffer1 -> {
//            Dump.parseOffsetsDump(buffer1.get(0), map2);
//        });
//
//        log.info("map3 {} {}", map3, System.currentTimeMillis() - startTime);
//
//        assertEquals(map, map2);
//    }


//    @Test
//    public void testDumpRead() throws IOException {
//
//        Map<TopicPartition, TopicPartitionOffset> map = new ConcurrentHashMap<>();
//
//        long startTime = System.currentTimeMillis();
////        String offsetDumpFile = "c:\\tmp\\matching\\offset-19841-0.log";
//        String offsetDumpFile = "c:\\tmp\\matching\\offset-19841-1.log";
//
//        Optional<List<ByteBuffer>> buffer2Opt = Dump.readOffsetFileChannel(offsetDumpFile);
//        assertTrue(buffer2Opt.isPresent());
//        buffer2Opt.ifPresent(buffer1 -> {
//            Dump.parseOffsetsDump(buffer1.get(buffer1.size()-1), map);
//        });
//
//        log.info("map {} {} {}", buffer2Opt.get().size(), map, System.currentTimeMillis() - startTime);
//
//    }


    //    @Test
    public void testFileChannel(int N) throws IOException {
        List<OrderEvent> testQueues = new ArrayList<>(10_0000);

        // test only
        for (int i = 0; i < N; i++) {
            OrderEvent orderEvent = new OrderEvent();
            testQueues.add(orderEvent);
        }

        long startTime = System.nanoTime();

        int n = 0;

        String dumpOrderBookFile = String.format("c:\\tmp\\matching\\xx-data-dump.log");
        Path path = new File(dumpOrderBookFile).toPath();
        //Path tmpPath = new File(dumpOrderBookFile + ".tmp").toPath();
        try (FileChannel fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            int limit = 0;
            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            ByteBuffer buffer = ByteBuffer.allocate(10240 + 200);
            for (OrderEvent orderEvent : testQueues) {
                limit += orderEvent.writeTo(buffer);
                n++;
                if (limit >= 10240) {
                    lenBuffer.putInt(limit);
                    lenBuffer.flip();
                    fileChannel.write(lenBuffer);
                    lenBuffer.rewind();

                    buffer.limit(limit);
                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.rewind();

                    limit = 0;
                }
            }
            if (limit > 0) {
                lenBuffer.putInt(limit);
                lenBuffer.flip();
                fileChannel.write(lenBuffer);
                lenBuffer.rewind();

                buffer.limit(limit);
                buffer.flip();
                fileChannel.write(buffer);
                buffer.rewind();
            }
            fileChannel.force(true);
            //Utils.atomicMoveWithFallback(tmpPath, path);

            log.info("testFileChannel XXX {} {}", n, (System.nanoTime() - startTime) / 1000000);
        }

    }

    @Test
    public void testBufferedWriter() throws IOException {
        List<OrderEvent> testQueues = new ArrayList<>(10_0000);

        // test only
        int N = 100_0000;
        for (int i = 0; i < N; i++) {
            OrderEvent orderEvent = new OrderEvent();
            testQueues.add(orderEvent);
        }

        long startTime = System.currentTimeMillis();

        int n = 0;

        String filepath = String.format("c:\\tmp\\matching\\buffered-writer.log");
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filepath))) {
            int limit = 0, count = 0;
            ByteBuffer buffer = ByteBuffer.allocate(10240 + 200);
            buffer.putInt(0);
            for (int i = 0; i < testQueues.size(); i++) {
                OrderEvent orderEvent = testQueues.get(i);
                limit += orderEvent.writeTo(buffer);
                count++;
                if (limit >= 10240 || i == testQueues.size() - 1) {
                    buffer.putInt(0, count);
                    buffer.flip();
                    String s = Utils.utf8(buffer);
                    //bufferedWriter.write(s.length());
                    bufferedWriter.write(s);
                    bufferedWriter.newLine();
                    buffer.rewind();
                    buffer.putInt(0);

                    n += count;
                    //log.error("write {} {}", s.length(), count);
                    limit = 0;
                    count = 0;
                }
            }

            bufferedWriter.flush();
            //Utils.atomicMoveWithFallback(tmpPath, path);
        }

        log.info("testBufferedWriter XXX {} {}", n, (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testReadOrderBookBufferedReader() throws IOException {

        long startTime = System.currentTimeMillis();

        String filePath = String.format("C:\\tmp\\matching/orderbook-topic05-0-1714439908001-0.log");
        List<OrderEvent> list = new ArrayList<>();
        Dump.readOrderBookDumpBufferedReader(filePath, list);

        log.info("testBufferedReader {} {} {}", filePath, list.size(), (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testReadOrderBookBufferedReaderCallback() throws IOException {
        //testBufferedWriter(100_0000);

        long startTime = System.currentTimeMillis();

        String filePath = String.format("C:\\tmp\\matching/orderbook-topic05-0-1714441218589-1.log");
        Dump.readOrderBookDumpBufferedReader(filePath, event -> {

        });

        log.info("testBufferedReader {} {}", filePath, (System.currentTimeMillis() - startTime));
    }


    //    @Test
    public void compare() throws IOException, InterruptedException {

        int N = 100_0000;
        for (int i = 0; i < 10; i++) {
            Thread.sleep(2000);

            testFileChannel(N);
            Thread.sleep(2000);

            testBufferedWriter();
            Thread.sleep(2000);
        }
    }

    @Test
    public void t() {
        String dumpOrderBookFile = String.format("C:\\tmp\\matching/test-orderbook-1.log");


        List<OrderEvent> testQueues = Lists.newArrayList();
//        testQueues.add(new OrderEvent());
        testQueues.add(new OrderEvent());
        Dump.writeOrderBookDumpBufferedWritter(dumpOrderBookFile, testQueues);

        List<OrderEvent> events1 = Lists.newArrayList();
        List<OrderEvent> events2 = Lists.newArrayList();
        Dump.readOrderBookDumpBufferedReader(dumpOrderBookFile, events1);
        Dump.readOrderBookDumpBufferedReader(dumpOrderBookFile, event -> events2.add(event));

        //assertEquals(testQueues, events1, "orderBook buffered writer and reader not equal");
        assertEquals(events1, events2, "orderBook buffered reader not equal");
    }

    @Test
    public void testReadOrderBookFileChannel() throws IOException {
        long startTime = System.currentTimeMillis();

//        String dumpOrderBookFile = String.format("C:\\tmp\\matching/orderbook-topic05-0-1714441218589-1.log");
        String dumpOrderBookFile = String.format("C:\\tmp\\matching/test-orderbook-1.log");

        List<OrderEvent> events = Lists.newArrayList();

        List<OrderEvent> testQueues = Lists.newArrayList();
//        testQueues.add(new OrderEvent());
        testQueues.add(new OrderEvent());
        Dump.writeOrderBookDumpBufferedWritter(dumpOrderBookFile, testQueues);

        Path path = new File(dumpOrderBookFile).toPath();
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {

            ByteBuffer lenBuffer = ByteBuffer.allocate(8);
            ByteBuffer newlineBuffer = ByteBuffer.allocate(System.lineSeparator().length());

            int BUFFER_SIZE = 10240;
            ByteBuffer BUFFER = ByteBuffer.allocate(BUFFER_SIZE);

            int total = 0;

            int len = fileChannel.read(lenBuffer);
            while (len > 0) {
                lenBuffer.flip();
                int sz = lenBuffer.getInt();
                int bytes = lenBuffer.getInt();

                ByteBuffer buffer;
//                if (bytes < BUFFER_SIZE)
//                    buffer = BUFFER;
//                else
                buffer = ByteBuffer.allocate(bytes);

                //buffer.rewind();
                //buffer.limit(bytes);
                fileChannel.read(buffer);
                buffer.flip();

//                Utils.utf8(buffer)


                for (int i = 0; i < sz; i++) {
                    OrderEvent event = new OrderEvent();
                    event.readFrom(buffer);
                    events.add(event);
                }

                total += sz;

                // skip newline
                newlineBuffer.rewind();
                int read = fileChannel.read(newlineBuffer);

                lenBuffer.rewind();
                len = fileChannel.read(lenBuffer);
            }

            log.info("testFileChannel XXX {} {}", total, (System.currentTimeMillis() - startTime) / 1000000);
        }
    }



    @Test
    public void testOrderBookFileChannel() throws IOException {
        String dumpOrderBookFile = String.format("C:\\tmp\\matching\\test-orderbook-filechannel.log");

        List<Event> testQueues = testOrderEventQueue(10_0000);
        List<Event> parsedQueues = Lists.newArrayList();

        //Dump.writeOrderBookByFileChannel(dumpOrderBookFile, testQueues);
        dumpOrderBookFile = "C:\\tmp\\matching\\orderbook+1714903075652+topic05+0+3033674+3033674+orderResults05+3+12236+ETH1+1.log";
        Dump.readOrderBookByFileChannel(dumpOrderBookFile, parsedQueues, null);

    }


}
