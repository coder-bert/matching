package com.mybit.matching.core.sender;

public class SenderFactory {
    public static Sender createSender() {
        return new ProducerSender();
    }
}
