package com.mybit.matching.core.entity;

import com.mybit.matching.core.dump.ByteBufferSerializer;
import com.mybit.matching.core.utils.UUID;
import com.mybit.matching.core.utils.Utils;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/**
 * 委托单
 *
 */

@Data
public class OrderEvent extends Event {

    private Long orderId;   // 委托单ID
    private Long price;     // 价格
    private Long money;     //

    private Long money1;     //
    private Long money2;     //
    private Long money3;     //
    private Long money4;     //
    private Long money5;     //
    private Long money6;     //
    private Long money7;     //
    private Long money8;     //
    private Long money9;     //
    private Long money10;     //

    String seq1 = UUID.randomUuid().toString(), seq2 = UUID.randomUuid().toString();
    String seq3 = UUID.randomUuid().toString(), seq4 = UUID.randomUuid().toString();
    String seq5 = UUID.randomUuid().toString(), seq6 = UUID.randomUuid().toString();

    @Override
    public int writeTo(ByteBuffer buffer) {
        int remaining = buffer.remaining();

        super.writeTo(buffer);

        Utils.writeUtf8Str(buffer, seq1);
        Utils.writeUtf8Str(buffer, seq2);
        Utils.writeUtf8Str(buffer, seq3);
        Utils.writeUtf8Str(buffer, seq4);
        Utils.writeUtf8Str(buffer, seq5);
        Utils.writeUtf8Str(buffer, seq6);

        write(buffer, orderId);                               // 8
        write(buffer, price);                                 // 8
        write(buffer, money);

        write(buffer, money1);
        write(buffer, money2);
        write(buffer, money3);
        write(buffer, money4);
        write(buffer, money5);
        write(buffer, money6);
        write(buffer, money7);
        write(buffer, money8);
        write(buffer, money9);
        write(buffer, money10);

        return remaining - buffer.remaining();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        super.readFrom(buffer);

        seq1 = Utils.utf8WithLen(buffer);
        seq2 = Utils.utf8WithLen(buffer);
        seq3 = Utils.utf8WithLen(buffer);
        seq4 = Utils.utf8WithLen(buffer);
        seq5 = Utils.utf8WithLen(buffer);
        seq6 = Utils.utf8WithLen(buffer);

        orderId = buffer.getLong();
        price = buffer.getLong();
        money = buffer.getLong();

        money1 = buffer.getLong();
        money2 = buffer.getLong();
        money3 = buffer.getLong();
        money4 = buffer.getLong();
        money5 = buffer.getLong();
        money6 = buffer.getLong();
        money7 = buffer.getLong();
        money8 = buffer.getLong();
        money9 = buffer.getLong();
        money10 = buffer.getLong();
    }

}
