package com.mybit.matching.core.config;


import com.mybit.matching.core.utils.AssertUtil;
import lombok.Data;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.*;

@Data
public class Config {

    /**
     * 如果非空，表示限制了只能处理这些symbols
     */
    private volatile Collection<String> limitSymbols = new HashSet<>(1000);

    private volatile boolean autoCreateIfNotExist = true;   // 如果limitSymbols为空，且autoCreateIfNotExist=false，则不能创建orderbook，至少设置1个
    private int bufferSize = 1024;

    private volatile boolean isSendAsync = false;
    private volatile boolean isBatch = true;
    private int batchSize = 1024;

    private String dumpPath = "/tmp/exchange";
    private long minDumpInterval = 60;          // dump最小时间间隔，避免太频繁，单位：秒，最小60s
    private long dumpInterval = 300;            // dump时间间隔，单位：秒，最小300s

    private long consumeInterval;               // 消费轮训等待时长
    private List<String> consumerTopics;        // 消费列表
    private Map<String, List<Integer>> consumerTopicOffsets;    // 消费topic对应的partitions


    private String producerTopic;               // 发送topic
    private int sendRetryTimes;                 // 失败发送重试次数，建议-1
    private int retryQueueCapacity;             // 失败重试队列最大容量，超过的会提交失败

    private int waitingCloseTimeout;            // 等待结束超时时间，单位 ms

    private KafkaProperties kafkaProperties;

    private boolean isTest = false;             // 测试用
    private boolean isTestDiscard = false;      // 测试用-是否开启自动丢弃
    private boolean isTestConsumer = false;     // 测试用-是否开启测试consumer

    private Config() {
    }

    private static volatile Config instance = null;

    public static Config getInstance() {
        if (instance == null) {
            synchronized (Config.class) {
                if (instance == null) {
                    instance = new Config();
                }
            }
        }
        return instance;
    }

    public Properties getKafkaProducerProps() {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        Map<String, Object> map = kafkaProperties.buildProducerProperties(null);
        props.putAll(map);
        return props;
    }

    public Properties getKafkaConsumerProps() {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        Map<String, Object> map = kafkaProperties.buildConsumerProperties(null);
        props.putAll(map);
        return props;
    }

    public void setSendRetryTimes(Integer retry) {
        sendRetryTimes = retry >= 0 ? retry : Integer.MAX_VALUE;
    }

    public Long getConsumeInterval() {
        return consumeInterval;
    }

    public void setDumpPath(String dumpPath) {
        AssertUtil.notEmpty(dumpPath, "invalid dumpPath config");
        if (dumpPath.endsWith("/")) dumpPath = dumpPath.substring(0, dumpPath.length()-1);
        AssertUtil.notEmpty(dumpPath, "invalid dumpPath config");
        this.dumpPath = dumpPath;
    }

    public int getWaitingCloseTimeout() {
        if (waitingCloseTimeout <= 0) return 0;
        return waitingCloseTimeout;
    }

}
