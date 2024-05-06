package com.mybit.matching.app;

import com.alibaba.fastjson2.JSON;
import com.mybit.matching.core.entity.OrderEvent;
import com.mybit.matching.core.kafka.KafkaProducer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class KafkaProducerTest {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int THREADS = 2;                        // 并发线程数量
        int TOTAL = THREADS * 100_000_000;         // 总消息数量


        String TOPIC = "topic05";
        KafkaProducer kafkaProducer = new KafkaProducer("localhost:9094", TOPIC);
        kafkaProducer.startup();


//        List<String> symbols = List.of("ETH2");
        List<String> symbols = List.of("ETH1", "ETH2","ETH3", "ETH4","ETH5", "ETH6");


        for (int k=0; k<THREADS; k++) {
            new Thread(
                    () -> {

                        AtomicLong total = new AtomicLong(0);
                        AtomicLong count = new AtomicLong(0);


                        long start = System.nanoTime();

                        final long[] lastOffset = {0L,0L,0L,0L,0L,0L,0L,0L,0L,0L,0L};
                        for (int i=0; i<TOTAL/THREADS; i++) {

                            String symbol = symbols.get(ThreadLocalRandom.current().nextInt(symbols.size()));
                            OrderEvent orderEvent = new OrderEvent();
                            orderEvent.setOrderId(Long.valueOf(i));
                            orderEvent.setGroupKey(symbol);
                            orderEvent.setOffset(lastOffset[0]);


                            Future<RecordMetadata> x = kafkaProducer.asyncSend(symbol, JSON.toJSONString(orderEvent), new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                    if (e == null) {
                                        //log.info("onCompletion {} {} {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

                                        lastOffset[recordMetadata.partition()] = Math.max(lastOffset[recordMetadata.partition()], recordMetadata.offset());

                                        long n = count.incrementAndGet();
                                        if (n % 10000 == 0)
                                            System.out.println(Thread.currentThread().getId() + " send to" + " " + TOPIC + " " + symbol + " " + recordMetadata.offset());

                                    } else {
                                        log.error("发送失败");
                                    }
                                }
                            });
                        }

                        long elapse = System.nanoTime() - start;
                        long time = total.addAndGet(elapse);
                        long n = count.get();
                        System.out.println("lastOffset:" + lastOffset + "  qps: " + 1000_000_000L/(time/n));

//                        try {
//                            Thread.sleep(100);
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
                    }
            ).start();
        }


        Thread.sleep(30_000);
    }
}
