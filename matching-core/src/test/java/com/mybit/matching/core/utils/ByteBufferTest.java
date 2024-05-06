package com.mybit.matching.core.utils;

import com.mybit.matching.core.entity.OrderEvent;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class ByteBufferTest {

    @Test
    public void testByteBufferStr() {
        final int INIT_SIZE = 100;
        ByteBuffer buffer = ByteBuffer.allocate(INIT_SIZE);

        String key = null;
        Utils.writeUtf8Str(buffer, key);
        buffer.flip();
        String key2 = Utils.utf8WithLen(buffer);
        assertEquals(key2, "");

        key = "null";
        buffer.rewind();
        buffer.limit(INIT_SIZE);
        Utils.writeUtf8Str(buffer, key);
        buffer.flip();
        key2 = Utils.utf8WithLen(buffer);
        assertEquals(key2, "null");
    }

    @Test
    public void testOrderEvent() {
        final int INIT_SIZE = 1000;
        ByteBuffer buffer = ByteBuffer.allocate(INIT_SIZE);

        OrderEvent orderEvent = new OrderEvent();
        orderEvent.writeTo(buffer);

        buffer.flip();
        OrderEvent orderEvent2 = new OrderEvent();
        orderEvent2.readFrom(buffer);


        //assertEquals(orderEvent, orderEvent2);
    }

}
