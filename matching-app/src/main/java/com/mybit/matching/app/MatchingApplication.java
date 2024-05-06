package com.mybit.matching.app;

import com.mybit.matching.app.configuration.MatchingConfiguration;
import com.mybit.matching.core.EngineServer;
import com.mybit.matching.core.config.Config;
import com.mybit.matching.core.signal.StopSignalHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

@Slf4j
@SpringBootApplication
public class MatchingApplication implements ApplicationRunner, ApplicationListener<ContextClosedEvent> {

    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    MatchingConfiguration matchingConfig;
    

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(MatchingApplication.class, args);

        EngineServer.getInstance().waitUntilStop();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        startEngineServer();
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        //EngineServer.getInstance().close();
    }

    private void startEngineServer() {
        // 启动EngineServer
        Config config = Config.getInstance();

        config.setKafkaProperties(kafkaProperties);
        config.setLimitSymbols(matchingConfig.getLimitSymbols());
        config.setAutoCreateIfNotExist(matchingConfig.getAutoCreateIfNotExist());
        config.setBufferSize(matchingConfig.getQueueBufferSize());

        config.setDumpPath(matchingConfig.getDumpPath());
        config.setDumpInterval(matchingConfig.getDumpInterval());
        config.setMinDumpInterval(matchingConfig.getMinDumpInterval());

        config.setConsumerTopics(matchingConfig.getConsumerTopics());
        config.setConsumerTopicOffsets(matchingConfig.getConsumerTopicOffsets());
        config.setConsumeInterval(matchingConfig.getConsumeInterval());

        config.setProducerTopic(matchingConfig.getProducerTopic());
        config.setSendRetryTimes(matchingConfig.getSendRetryTimes());
        config.setRetryQueueCapacity(matchingConfig.getAsyncSendRetryCapacity());
        config.setBatch(matchingConfig.getIsBatch());
        config.setBatchSize(matchingConfig.getBatchSize());
        config.setSendAsync(matchingConfig.getIsSendAsync());

        config.setWaitingCloseTimeout(matchingConfig.getWaitingCloseTimeout());

        // test only
        config.setTest(matchingConfig.getIsTest());
        config.setTestDiscard(matchingConfig.getIsDiscard());
        config.setTestConsumer(matchingConfig.getIsTestConsumer());
        // test only


        EngineServer engineServer = EngineServer.getInstance();
        engineServer.init();
        engineServer.startup();

        // 注册退出信号
        StopSignalHandler signalHandler = new StopSignalHandler();
        signalHandler.register();

        //Runtime.getRuntime().addShutdownHook(new Thread(engineServer::close));

        // test only 开启消费线程，比对offset丢失结果
        if (config.isTest() && config.isTestConsumer()) {
            new Thread(() -> {
                try {
                    String resultTopic = config.getProducerTopic();
                    new KafkaConsumerTest().startConsume(resultTopic, true);
                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
                    log.error("InterruptedException ", e);
                }
            }).start();
        }
    }

}
