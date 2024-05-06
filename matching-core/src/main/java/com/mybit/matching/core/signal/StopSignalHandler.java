package com.mybit.matching.core.signal;

import com.mybit.matching.core.EngineServer;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.List;

@Slf4j
@SuppressWarnings("restriction")
public class StopSignalHandler implements SignalHandler {

    private final List<String> stopSignals = List.of("TERM", "INT");

    private boolean stoped = false;

    public void register() {
        // 注册对指定信号的处理
        stopSignals.forEach(signalName -> {
            Signal.handle(new Signal(signalName) , this);
        });
        //Signal.handle(new Signal("TERM") , this);    // kill or kill -15
        //Signal.handle(new Signal("INT"), this);      // kill -2
    }

    @Override
    public void handle(Signal signal) {
        if (stoped) {
            return;
        }
        stoped = true;
        
        // 信号量名称
        String signalName = signal.getName();
        // 信号量数值
        int sigNum = signal.getNumber();

        log.info("receive signal {} {}-{}", stopSignals, sigNum, signalName);
        
        if(stopSignals.contains(signalName)){
            EngineServer.getInstance().close();
            Runtime.getRuntime().exit(0);
        }
    }


}