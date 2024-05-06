package com.mybit.matching.core.dump;

import com.mybit.matching.core.entity.OrderEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

public class BaseByteBufferSerializer<T extends ByteBufferSerializer> {
    public int writeTo(T t, ByteBuffer buffer) {
        return t.writeTo(buffer);
    }

    public void readFrom(T t, ByteBuffer buffer) {
        t.readFrom(buffer);
    }
}
