package com.mybit.matching.core.main;

import com.mybit.matching.core.EngineServer;
import com.mybit.matching.core.config.Config;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * startup without spring
  */

@Slf4j
public class Main {

    public static void main_(String[] args) throws InterruptedException, IOException {

        Config config = Config.getInstance();

        EngineServer engineServer = EngineServer.getInstance();
        engineServer.init();
        engineServer.startup();

        while(engineServer.isRunning()) {
            Thread.sleep(1000);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(engineServer::close));
    }
}
