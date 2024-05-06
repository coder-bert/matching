package com.mybit.matching.core.entity;


import lombok.Data;

/**
 * 委托单撮合结果
 */

@Data
public class OrderResult extends EventResult {


    // todo ....


    public OrderResult() {
        this(0, "success");
    }

    public OrderResult(int code, String msg) {
        this(code, msg, null);
    }

    public OrderResult(int code, String msg, Event order) {
        super(code, msg, order);
    }

    public static OrderResult ok() {
        return new OrderResult(0, "success");
    }

    public static OrderResult of(int code, String msg, Event order) {
        return new OrderResult(code, msg, order);
    }


//    public static OrderResult of(ErrorCode errorCode, Event order) {
//        return new OrderResult(errorCode.getCode(), errorCode.getMsg());
//    }
//
//    public static OrderResult of(ErrorCode errorCode, String msg, Event order) {
//        return new OrderResult(errorCode.getCode(), msg, order);
//    }


}
