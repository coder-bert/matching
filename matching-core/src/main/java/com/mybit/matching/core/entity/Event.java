package com.mybit.matching.core.entity;


import com.mybit.matching.core.dump.ByteBufferSerializer;
import com.mybit.matching.core.utils.Utils;
import lombok.Data;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;

/**
 * 事件基类
 */

@Data
public class Event implements ByteBufferSerializer {
    protected Long startTime = System.nanoTime();

    private String seq;         // 唯一序列号（可能是orderId）
    private String symbol;      // 交易对
    private String groupKey;    //

    // 以下为kafka相关信息
    private String topic;       // 源kafka中记录的topic
    private Integer partition;  // 源kafka中记录的partition
    private Long offset;        // 源kafka中记录的offset
    private String key;         // 源kafka中记录的key
    private Headers headers;    // 源kafka记录headers

    @Override
    public int writeTo(ByteBuffer buffer) {
        write(buffer, startTime);

        Utils.writeUtf8Str(buffer, seq);
        Utils.writeUtf8Str(buffer, symbol);
        Utils.writeUtf8Str(buffer, groupKey);

        Utils.writeUtf8Str(buffer, topic);
        write(buffer, partition);
        write(buffer, offset);
        Utils.writeUtf8Str(buffer, key);

        // todo headers

        return 0;
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        startTime = buffer.getLong();

        seq = Utils.utf8WithLen(buffer);
        symbol = Utils.utf8WithLen(buffer);
        groupKey = Utils.utf8WithLen(buffer);

        topic = Utils.utf8WithLen(buffer);
        partition = buffer.getInt();
        offset = buffer.getLong();
        key = Utils.utf8WithLen(buffer);

        // todo headers
    }


    public void write(ByteBuffer buffer, Long l) {
        if (l == null) buffer.putLong(0);
        else buffer.putLong(l);
    }
    public void write(ByteBuffer buffer, Integer n) {
        if (n == null) buffer.putInt(0);
        else buffer.putInt(n);
    }

    public void write(ByteBuffer buffer, String s) {
        if (s == null) s = "";
        if (s.isEmpty()) {
            buffer.putInt(0);
        } else {
            byte[] b = Utils.utf8(s);
            buffer.putInt(b.length);
            buffer.put(b);
        }
    }


}
