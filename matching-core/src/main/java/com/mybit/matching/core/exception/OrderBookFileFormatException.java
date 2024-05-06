package com.mybit.matching.core.exception;

public class OrderBookFileFormatException extends BaseException {

    public OrderBookFileFormatException(String message, String fileName) {
        super(message + " fileName:" + fileName);
    }

    public OrderBookFileFormatException(Throwable cause) {
        super(cause);
    }

    public OrderBookFileFormatException() {
    }
}