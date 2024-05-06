package com.mybit.matching.core.entity;


import lombok.Data;
import org.apache.kafka.common.header.Headers;


@Data
public class EventResult {

    public static EventResult OK = new EventResult(0, "success");
    public static EventResult ERROR = new EventResult(-1, "error");

    private int code;
    private String msg;

    protected Long createTime = System.nanoTime();

    private String seq;         // 唯一序列号（可能是orderId）

    private String groupKey;    //

    // 以下为kafka相关信息
    private String topic;       // 源kafka中记录的topic
    private Integer partition;  // 源kafka中记录的partition
    private Long offset;        // 源kafka中记录的offset
    private String key;         // 源kafka中记录的key
    private Headers headers;    // 源kafka记录headers


    //private Event event;


    public EventResult() {
        this(0, "success");
    }

    public EventResult(int code, String msg) {
        this(code, msg, null);
    }

    public EventResult(int code, String msg, Event event) {
        this.code = code;
        this.msg = msg;
        //this.event = event;

        if (event != null) {
            this.createTime = event.getStartTime();
        }
    }

    public static boolean isOK(EventResult result) {
        return result != null && result.code == OK.code;
    }

    public void setEvent(Event event) {
        //this.event = event;

        if (event != null) {
            this.createTime = event.getStartTime();
        }
    }

}
