package com.mybit.matching.core.dump;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.disruptor.GroupDisruptor;
import com.mybit.matching.core.disruptor.GroupDisruptorManager;
import com.mybit.matching.core.entity.*;
import com.mybit.matching.core.exception.BaseException;
import com.mybit.matching.core.exception.OrderBookFileFormatException;
import com.mybit.matching.core.orderbook.FileNameRecord;
import com.mybit.matching.core.orderbook.OrderBook;
import com.mybit.matching.core.orderbook.OrderBookManager;
import com.mybit.matching.core.orderbook.TopicPartitionOffset;
import com.mybit.matching.core.sender.Sender;
import com.mybit.matching.core.utils.AssertUtil;
import com.mybit.matching.core.utils.FileTraversal;
import com.mybit.matching.core.utils.StringUtil;
import com.mybit.matching.core.utils.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;


@Slf4j
public class Dump {

    public static int VERSION = 123;

    // offset文本格式方便查看和手动指定消费位置（回退）
    // N表示后续记录数量
    // <magic>,version,curTimestamp(ms),<N1>,<N2>,[srcTopic,srcPartition,matchingOffset,sentOffset,dstTopic,dstPartition,dstSentOffset] ,...,<end>
    // todo: crc32

    public static final int magic = 0x01EF;
    private static final String OFFSET_HEADER_FORMAT = "%d,%d,%d,%d,%d";
    private static final int OFFSET_HEADER_LEN = 5;
    private static final String OFFSET_RECORD_FORMAT = ",%s,%d,%d,%d,%s,%d,%d";
    private static final int OFFSET_RECORD_LEN = 7;
    private static final String OFFSET_FILE_LIST_FORMAT = ",%s,%s";
    private static final int OFFSET_FILE_LIST_LEN = 7;

    @Data
    public static class OffsetMetadata {
        int magic;
        int vertion;
        long timeStamp;
        Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap;
        Map<String, String> symbolDumpFiles;
    }


    public static String writeOffsetsBufferedWriter(long fileTimeStamp, final Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, Map<String, String> symbolDumpFiles) {
        String header = String.format(OFFSET_HEADER_FORMAT, magic, Dump.VERSION, fileTimeStamp, topicPartitionOffsetMap.size(), symbolDumpFiles.size());
        StringBuilder sb = new StringBuilder(header);

        topicPartitionOffsetMap.forEach((tp, tpOffset) -> {
            TopicPartition srcTp = tpOffset.getSrcTp();
            TopicPartition dstTp = tpOffset.getDstTp();

            sb.append(String.format(OFFSET_RECORD_FORMAT, srcTp.topic(), srcTp.partition(), tpOffset.getSrcMatchedOffset(), tpOffset.getSrcSentOffset(), dstTp.topic(), dstTp.partition(), tpOffset.getDstSentOffset()));
        });

        symbolDumpFiles.forEach((symbol, orderBookFile) -> {
            sb.append(String.format(OFFSET_FILE_LIST_FORMAT, symbol, orderBookFile));
        });

        return sb.toString();
    }

    public static boolean parseOffsetsDump(String data, Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, Map<String, String> symbolDumpFiles) {
        if (data == null || data.isEmpty()) return false;

        String sMagic = String.format("%d,", magic);
        if (!data.startsWith(sMagic)) {
            log.error("invalid magic {}", data);
            return false;
        }

        List<String> lst = Splitter.on(",").splitToList(data);
        if (lst.size() < OFFSET_HEADER_LEN) {
            log.error("invalid offset dump format {}", data);
            return false;
        }
        // magic 0
        int version = Integer.parseInt(lst.get(1));
        long timestamp = Long.parseLong(lst.get(2));
        int N1 = Integer.parseInt(lst.get(3));
        int N2 = Integer.parseInt(lst.get(4));

        int idx = OFFSET_HEADER_LEN;

        Map<TopicPartition, TopicPartitionOffset> map = new HashMap<>(N1);
        for (int i = 0; i < N1; i++) {
            String srcTopic = lst.get(idx++);
            int srcPatition = Integer.parseInt(lst.get(idx++));
            long srcMathingOffset = Long.parseLong(lst.get(idx++));
            long srcSentOffset = Long.parseLong(lst.get(idx++));
            String dstTopic = lst.get(idx++);
            int dstPatition = Integer.parseInt(lst.get(idx++));
            long dstSentOffset = Long.parseLong(lst.get(idx++));
            long updateTime = Long.parseLong(lst.get(idx++));

            TopicPartitionOffset tpOffset = new TopicPartitionOffset(srcTopic, srcPatition, srcMathingOffset, srcSentOffset,
                    dstTopic, dstPatition, dstSentOffset,
                    updateTime);

            map.put(new TopicPartition(srcTopic, srcPatition), tpOffset);
        }

        if (topicPartitionOffsetMap != null) {
            topicPartitionOffsetMap.clear();
            topicPartitionOffsetMap.putAll(map);
        }

        Map<String, String> symbolDumpFiles_ = new HashMap<>(N2);
        for (int i = 0; i < N2; i++) {
            String symbol = lst.get(idx++);
            String dumpFile = lst.get(idx++);
            symbolDumpFiles_.put(symbol, dumpFile);
        }

        if (symbolDumpFiles != null) {
            symbolDumpFiles.clear();
            symbolDumpFiles.putAll(symbolDumpFiles_);
        }

        return true;
    }

    public static boolean writeOffsetsBufferedWriter(String dumpFile, long fileTimeStamp, final Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, final Map<String, String> symbolDumpFiles) throws IOException {

        Path path = new File(dumpFile).toPath();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path.toFile(), true))) {
            // 文本格式为了方便查看和手动管理
            String offsetsData = writeOffsetsBufferedWriter(fileTimeStamp, topicPartitionOffsetMap, symbolDumpFiles);

            bufferedWriter.write(offsetsData);
            bufferedWriter.newLine();

            return true;
        } catch (IOException e) {
            //throw new RuntimeException(e);
            log.error("IOException {}", dumpFile, e);
        }
        return false;
    }

    public static boolean readOffsetsBufferedReader(String dumpFile, Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, Map<String, String> symbolDumpFiles) throws IOException {
        Path path = new File(dumpFile).toPath();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path.toFile()))) {
            String lastLineNotEmpty = null;
            while (true) {
                String line = bufferedReader.readLine();
                if (StringUtil.isEmpty(line)) {
                    break;
                }
                lastLineNotEmpty = line;
            }
            if (StringUtil.isNotEmpty(lastLineNotEmpty)) {
                return parseOffsetsDump(lastLineNotEmpty, topicPartitionOffsetMap, symbolDumpFiles);
            }
        } catch (IOException e) {
            //throw new RuntimeException(e);
            log.error("IOException {}", dumpFile, e);
        }
        return false;
    }

//    public static ByteBuffer dumpOffsetsBuffer(final Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, long now) {
//        int size = topicPartitionOffsetMap.size();
//        ByteBuffer buffer = ByteBuffer.allocate(4 * 3 + 8 + size * 200);
//        buffer.putInt(magic).putLong(now).putInt(Dump.VERSION).putInt(size);
//
//        AtomicInteger limit = new AtomicInteger(buffer.position());
//        topicPartitionOffsetMap.forEach((tp, tpOffset) -> {
//            limit.addAndGet(tpOffset.writeTo(buffer));
//        });
//
//        return buffer.limit(limit.get());
//    }

//    public static boolean parseOffsetsDump(ByteBuffer buffer, Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap) {
//        buffer.flip();
//
//        int magic_ = buffer.getInt();
//        long timestamp = buffer.getLong();
//        int version = buffer.getInt();
//        int size = buffer.getInt();
//
//        Map<TopicPartition, TopicPartitionOffset> map = new HashMap<>(size);
//
//        for (int i = 0; i < size; i++) {
//            TopicPartitionOffset tpOffset = new TopicPartitionOffset();
//            tpOffset.readFrom(buffer);
//
//            map.put(tpOffset.getSrcTp(), tpOffset);
//        }
////        log.info("{}", map);
//
//        if (topicPartitionOffsetMap != null) {
//            topicPartitionOffsetMap.clear();
//            topicPartitionOffsetMap.putAll(map);
//        }
//
//        return true;
//    }


//    public static void writeOffsetFileChannel(String dumpFile, ByteBuffer buffer) throws IOException {
//        Path path = new File(dumpFile).toPath();
//        //Path tmpPath = new File(dumpFile + ".tmp").toPath();
//        try (FileChannel fileChannel = FileChannel.open(path,
//                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
//
//            // 移到结尾  StandardOpenOption.APPEND
//            //fileChannel.position(fileChannel.size());
////            fileChannel.write()
//
//            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
//            lenBuffer.putInt(buffer.position());
//            lenBuffer.flip();
//            fileChannel.write(lenBuffer);
//            lenBuffer.rewind();
//
//            buffer.flip();
//            int writen = fileChannel.write(buffer);
//
//            fileChannel.force(true);
//        }
//
//        //Utils.atomicMoveWithFallback(tmpPath, path);
//    }
//
//    public static Optional<List<ByteBuffer>> readOffsetFileChannel(String offsetFile) throws IOException {
//        // Checking for empty files.
//        if (offsetFile == null || offsetFile.isEmpty()) {
//            return Optional.empty();
//        }
//
//        List<ByteBuffer> byteBuffers = new ArrayList<>();
//        try (ReadableByteChannel channel = Channels.newChannel(new FileInputStream(offsetFile))) {
//            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
//            while (true) {
//                // Read length
//                int lenBufferReadCt;
//                // Returns: The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
//                if ((lenBufferReadCt = channel.read(lenBuffer)) > 0) {
//                    lenBuffer.rewind();
//
//                    if (lenBufferReadCt != lenBuffer.capacity()) {
//                        throw new IOException("Invalid amount of data read for the length of an entry, file may have been corrupted.");
//                    }
//                }
//
//                if (lenBufferReadCt < 0) {
//                    break;
//                }
//
//                // Read the length of each entry
//                final int len = lenBuffer.getInt();
//                lenBuffer.rewind();
//
//                // Read the entry
//                ByteBuffer buffer = ByteBuffer.allocate(len);
//                final int read = channel.read(buffer);
//                if (read != len) {
//                    throw new IOException("Invalid amount of data read, file may have been corrupted.");
//                }
//                byteBuffers.add(buffer);
//            }
//
//            return Optional.of(byteBuffers);
//        }
//    }


    public static boolean writeOrderBookDumpBufferedWritter(String dumpFile, List<OrderEvent> queues) {
        Path path = new File(dumpFile).toPath();
        Path tmpPath = new File(dumpFile + ".tmp").toPath();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tmpPath.toFile()))) {
            int bytes = 0, size = 0;
            ByteBuffer buffer = ByteBuffer.allocate(10240 + 300);
            buffer.putInt(0);   // size
            buffer.putInt(0);   // bytes

            for (int i = 0; i < queues.size(); i++) {
                OrderEvent orderEvent = queues.get(i);
                bytes += orderEvent.writeTo(buffer);
                size++;
                if (bytes >= 10240 || i == queues.size() - 1) {
                    buffer.putInt(0, size);
                    buffer.putInt(4, bytes);
                    buffer.flip();

                    // 转换前后字节是错误的
                    String s = Utils.utf8(buffer);
                    //bufferedWriter.write(s.length());
                    bufferedWriter.write(s);
                    bufferedWriter.newLine();
                    buffer.rewind();
                    buffer.putInt(0);
                    buffer.putInt(0);

                    bytes = 0;
                    size = 0;
                }
            }

            bufferedWriter.flush();
            bufferedWriter.close();
            Utils.atomicMoveWithFallback(tmpPath, path);
            return true;
        } catch (IOException e) {
            //throw new RuntimeException(e);
            log.error("IOException {}", dumpFile, e);
        }
        return false;
    }

    public static boolean readOrderBookDumpBufferedReader(String filePath, LoadCallback callback) {

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath))) {
            long start = System.currentTimeMillis();
            while (true) {
                int bytes = bufferedReader.read();
                String line = bufferedReader.readLine();
                if (line == null) break;
                if (line.isEmpty()) continue;

                ByteBuffer buffer = ByteBuffer.wrap(Utils.utf8(line));

                int sz = buffer.getInt();
                bytes = buffer.getInt();

                for (int i = 0; i < sz; i++) {
                    OrderEvent orderEvent = new OrderEvent();
                    orderEvent.readFrom(buffer);

                    callback.onData(orderEvent);
                }
            }
            log.info("xfjasdasfdlkasjdfkjasfj {}", System.currentTimeMillis() - start);
            return true;
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException {}", filePath, e);
        } catch (IOException e) {
            log.error("IOException {}", filePath, e);
        }
        return false;
    }

    public static boolean readOrderBookDumpBufferedReader(String filePath, List<OrderEvent> list) {
        final int defaultCharBufferSize = 8192;
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath), defaultCharBufferSize*10)) {
            while (true) {
                //int bytes = bufferedReader.read();
                String s = bufferedReader.readLine();
                if (s == null) break;
                ByteBuffer buffer = ByteBuffer.wrap(Utils.utf8(s));

                int sz = buffer.getInt();
                int bytes = buffer.getInt();

                for (int i = 0; i < sz; i++) {
                    OrderEvent orderEvent = new OrderEvent();
                    orderEvent.readFrom(buffer);

                    list.add(orderEvent);
                }
            }
            return true;
        } catch (FileNotFoundException e) {
            log.error("FileNotFoundException {}", filePath, e);
        } catch (IOException e) {
            log.error("IOException {}", filePath, e);
        }
        return false;
    }


//    public static ByteBuffer dumpOffsetsBuffer(final Map<TopicPartition, TopicPartitionOffset> topicPartitionOffsetMap, long now) {
//        int size = topicPartitionOffsetMap.size();
//        ByteBuffer buffer = ByteBuffer.allocate(4 * 3 + 8 + size * 200);
//        buffer.putInt(magic).putLong(now).putInt(Dump.VERSION).putInt(size);
//
//        AtomicInteger limit = new AtomicInteger(buffer.position());
//        topicPartitionOffsetMap.forEach((tp, tpOffset) -> {
//            limit.addAndGet(tpOffset.writeTo(buffer));
//        });
//
//        return buffer.limit(limit.get());
//    }

    public static boolean writeOrderBookByFileChannel(String dumpFile, List<Event> events) {
        long start = System.currentTimeMillis();

        int total = 0;

        Path path = new File(dumpFile).toPath();
        Path tmpPath = new File(dumpFile + ".tmp").toPath();
        try (FileChannel fileChannel = FileChannel.open(tmpPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

            final int HEADER_SIZE = 8;
            final int BUFFER_SIZE = 1024 * 30;
            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            ByteBuffer buffer = ByteBuffer.allocate((int) (BUFFER_SIZE * 1.5));
            ByteBuffer[] buffers = {headerBuffer, buffer};

            int bytes = 0, sz = 0, eventSize = events.size();

            for (int i = 0; i < eventSize; i++) {
                Event orderEvent = events.get(i);

                bytes += orderEvent.writeTo(buffer);
                sz++;

                if (bytes >= BUFFER_SIZE || i == eventSize - 1) {
                    headerBuffer.putInt(sz);
                    headerBuffer.putInt(bytes);
                    headerBuffer.flip();
                    buffer.flip();
                    fileChannel.write(buffers);
                    headerBuffer.rewind();
                    buffer.rewind();

                    total += sz;
                    bytes = sz = 0;
                }
            }
            Utils.atomicMoveWithFallback(tmpPath, path);

            return true;
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }

        log.info("writeOrderBookByFileChannel save {} {} {}", total, dumpFile, System.currentTimeMillis() - start);

        return false;
    }

    public static boolean readOrderBookByFileChannel(String filePath, LoadCallback callback) {
        return readOrderBookByFileChannel(filePath, null, callback);
    }
    public static boolean readOrderBookByFileChannel(String filePath, List<Event> events, LoadCallback callback) {
        long start = System.currentTimeMillis();

        int total = 0;

        Path path = new File(filePath).toPath();
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
//        try (FileInputStream input = new FileInputStream(filePath)) {
//            FileChannel fileChannel = input.getChannel();

            final int HEADER_SIZE = 8;
            final int BUFFER_SIZE = 1024 * 50;
            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            ByteBuffer BUFFER = ByteBuffer.allocate(BUFFER_SIZE);

            while (true) {
                headerBuffer.rewind();
                int read = fileChannel.read(headerBuffer);
                if (read < 0) {
                    break;
                } else if (read < HEADER_SIZE) {
                    throw new OrderBookFileFormatException("invalid orderBook file format: header.", filePath);
                }

                headerBuffer.flip();
                int size = headerBuffer.getInt();
                int bytes = headerBuffer.getInt();

                ByteBuffer buffer = bytes > BUFFER_SIZE ? ByteBuffer.allocate(bytes) : BUFFER;

                buffer.rewind();
                buffer.limit(bytes);
                read = fileChannel.read(buffer);
                if (read != bytes) {
                    throw new OrderBookFileFormatException("invalid orderBook file format: record.", filePath);
                }
                buffer.flip();

                for (int i=0; i<size; i++) {
                    OrderEvent event = new OrderEvent();
                    event.readFrom(buffer);

                    total ++;

                    if (events != null) {
                        events.add(event);
                    }
                    if (callback != null)
                        callback.onData(event);
                }
            }
        } catch (IOException e) {
            throw new OrderBookFileFormatException(e);
        }

        log.info("readOrderBookDumpFileChannel load {} {} {}", total, filePath, System.currentTimeMillis() - start);
        return true;
    }


    /**
     * 加载最新的dump文件，并实现kafka续传
     */
    public static void loadLastestDump() {
        Config config = Config.getInstance();
        GroupDisruptorManager groupDisruptorManager = GroupDisruptorManager.getInstance();

        // 启动，镜像加载
        String dumpPath = config.getDumpPath();
        Collection<String> orderBookDumpFiles = FileTraversal.enumFiles(dumpPath, null, "orderbook", ".log");

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
            }
        });


        // 最新dump
        Map<TopicPartition, List<FileNameRecord>> topicPartitionListMap = Maps.newHashMap();
        if (!sortedMap.isEmpty()) {
            List<FileNameRecord> latestFiles = sortedMap.lastEntry().getValue();
            latestFiles.forEach(record -> {
                TopicPartition tp = new TopicPartition(record.getTopic(), record.getPartition());       // 跟groupKey一致
                List<FileNameRecord> records = topicPartitionListMap.computeIfAbsent(tp, t -> new ArrayList<>());
                records.add(record);
            });
        }


        long startTime = System.currentTimeMillis();

        final String producerTopic = config.getProducerTopic();
        final List<String> consumerTopics = config.getConsumerTopics();
        final Map<String, List<Integer>> consumerTopicOffsets = config.getConsumerTopicOffsets();   // 消费topic对应的partitions

        Collection<TopicPartitionOffset> subscribedTpAndOffsets = new ArrayList<>();

        // 根据设置的消费topic设置消费进度（可能有新增和删除，以配置的为准）
        consumerTopics.forEach(topic -> {
            List<Integer> subscribePartitions = consumerTopicOffsets.get(topic);
            if (subscribePartitions == null || subscribePartitions.isEmpty()) {
                // 默认只有1个partition，如果未配置则初始化为0，否则需要在配置文件中配置
                subscribePartitions = List.of(0);
            }
            for (Integer partition : subscribePartitions) {
                TopicPartition tp = new TopicPartition(topic, partition);
                TopicPartitionOffset topicPartitionOffset = setConsumeOffset(tp, producerTopic, topicPartitionListMap);
                subscribedTpAndOffsets.add(topicPartitionOffset);

                // 是否存在dump文件，如果存在则加载最新的（第一个是最新的）
                List<FileNameRecord> fileNameRecords = topicPartitionListMap.get(tp);
                if (fileNameRecords != null && !fileNameRecords.isEmpty()) {
                    String groupKey = tp.toString();
                    GroupDisruptor groupDisruptor = groupDisruptorManager.getDisruptorOrCreate(groupKey, tp);
                    if (groupDisruptor == null) {
                        log.error("create disruptor failed {}", groupKey);
                        throw new BaseException("create disruptor failed groupKey:" + groupKey);
                    }
                    // 通过LoadEvent，多线程并发加载
                    LoadEvent loadEvent = new LoadEvent(topicPartitionOffset, fileNameRecords);
                    log.info("load dump elapse {} {} {}", groupKey, System.identityHashCode(loadEvent), startTime);
                    groupDisruptor.fireEvents(groupKey, Collections.singleton(loadEvent), startTime);
                }
            }
        });

        subscribedTpAndOffsets.forEach(r -> {
            log.info("the latest offset read from dump file {} {} {}", r.getSrcTp().toString(), r.getSrcMatchedOffset(), r.getSrcSentOffset());
        });
        OrderBookManager orderBookManager = OrderBookManager.getInstance();
        orderBookManager.setTopicPartitionOffsets(subscribedTpAndOffsets);
    }

    /**
     * 设置消费进度
     *
     * @param tp
     * @param producerTopic
     * @param topicPartitionListMap
     */
    private static TopicPartitionOffset setConsumeOffset(TopicPartition tp, String producerTopic,
                                  Map<TopicPartition, List<FileNameRecord>> topicPartitionListMap) {
        if (topicPartitionListMap.containsKey(tp)) {
            FileNameRecord record = topicPartitionListMap.get(tp).get(0);

            int dstPartition = record.getDstPartition();
            long dstSentOffset = record.getDstSentOffset();
            if (!producerTopic.equals(record.getDstTopic())) {
                dstPartition = -1;
                dstSentOffset = -1;
                log.warn("目标topic发生变更，请确认是否正确，topic变更会导致记录的offset发送变化 {} -> {}", record.getDstTopic(), producerTopic);
            }
            return new TopicPartitionOffset(tp.topic(), tp.partition(), record.getMatchedOffset(), record.getSentOffset(), producerTopic, dstPartition, dstSentOffset, record.getTimestamp());
        } else {
            return new TopicPartitionOffset(tp.topic(), tp.partition(), -1, -1, producerTopic, -1, -1, -1);
        }
    }

}
