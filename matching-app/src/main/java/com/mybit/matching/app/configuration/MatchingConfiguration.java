package com.mybit.matching.app.configuration;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "matching")
public class MatchingConfiguration {

    List<String> limitSymbols;
    Boolean autoCreateIfNotExist;
    Integer queueBufferSize;

    String dumpPath;
    Integer minDumpInterval = 60;       // dump最小时间间隔，避免太频繁，单位：秒，最小60s
    Integer dumpInterval = 300;         // dump时间间隔，单位：秒，最小300s

    Boolean isSendAsync = true;
    Boolean isBatch = true;
    Integer batchSize = 1024;

    List<String> consumerTopics;        // 消费topic列表
    Map<String, List<Integer>> consumerTopicOffsets;    // 消费topic对应的partitions
    Integer consumeInterval;            // 消费轮训时长
    String producerTopic;               // 发送topic
    Integer sendRetryTimes;             // [同步+异步] 发送失败重试次数，-1:一直重试
    Integer asyncSendRetryCapacity;     // [异步] 发送重试队列容量 <=0 表示禁用重试，重试次数sendRetryTimes

    Integer waitingCloseTimeout;        // 等待退出超时时间，ms

    Boolean isTest;                     // 测试用
    Boolean isDiscard;                  // 测试用
    Boolean isTestConsumer;             // 测试用
}


