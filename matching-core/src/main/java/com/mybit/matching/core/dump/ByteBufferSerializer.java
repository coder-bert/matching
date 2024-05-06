package com.mybit.matching.core.dump;

import java.nio.ByteBuffer;

public interface ByteBufferSerializer extends Serializer {

    int writeTo(ByteBuffer buffer);
    void readFrom(ByteBuffer buffer);
}
