package com.mybit.matching.core.orderbook;

import com.mybit.matching.core.dump.ByteBufferSerializer;
import com.mybit.matching.core.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.TopicPartition;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicPartitionOffset implements ByteBufferSerializer {
    private TopicPartition srcTp;
    private Long srcMatchedOffset;
    private Long srcSentOffset;

    private TopicPartition dstTp;
    private Long dstSentOffset;
    private long updateTime;

    public TopicPartitionOffset(String srcTopic, int srcPartition, long srcMatchedOffset, long srcSentOffset,
                                String dstTopic, int dstPartition, long dstSentOffset,
                                long updateTime) {
        this.srcTp = new TopicPartition(srcTopic, srcPartition);
        this.srcMatchedOffset = srcMatchedOffset;
        this.srcSentOffset = srcSentOffset;
        this.dstTp = new TopicPartition(dstTopic, dstPartition);
        this.dstSentOffset = dstSentOffset;
        this.updateTime = updateTime;
    }

    public TopicPartitionOffset(TopicPartitionOffset o) {
        if (o == null || o == this) {
            return;
        }
        this.srcTp = new TopicPartition(o.srcTp.topic(), o.srcTp.partition());
        this.srcMatchedOffset = o.srcMatchedOffset;
        this.srcSentOffset = o.srcSentOffset;
        this.dstTp = new TopicPartition(o.dstTp.topic(), o.dstTp.partition());
        this.dstSentOffset = o.dstSentOffset;
        this.updateTime = o.updateTime;
    }
    /**
     * 根据记录位置，返回kafka consumer消费的位置
     * @return
     */
    public Long getToConsumingOffset() {
        if (srcSentOffset != null && srcSentOffset >= 0) {
            return srcSentOffset+1;         // 成功发送位置的下一位置
        }

        if (dstSentOffset != null && dstSentOffset >= 0) {
            // todo 消费下游kafka获取
        }

        return -1L;  // kafkaConsumer.seek的offset不能<0
    }

    @Override
    public int writeTo(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        // 长度: 5*8 + 2*topic 假设200个
        byte[] bytes = Utils.utf8(srcTp.topic());
        buffer.putInt(bytes.length);                                    // 4
        buffer.put(bytes);                                              // <100
        buffer.putInt(srcTp.partition());                               // 4
        buffer.putLong(srcMatchedOffset);                               // 8
        buffer.putLong(srcSentOffset);                                  // 8

        bytes = Utils.utf8(dstTp.topic());
        buffer.putInt(bytes.length);                                    // 4
        buffer.put(bytes);                                              // <100
        buffer.putInt(dstTp.partition());                               // 4
        buffer.putLong(dstSentOffset);                                  // 8

        buffer.putLong(updateTime);                                     // 8

        return remaining - buffer.remaining();
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        srcTp = new TopicPartition(Utils.utf8WithLen(buffer), buffer.getInt());
        srcMatchedOffset = buffer.getLong();
        srcSentOffset = buffer.getLong();

        dstTp = new TopicPartition(Utils.utf8WithLen(buffer), buffer.getInt());
        dstSentOffset = buffer.getLong();

        updateTime = buffer.getLong();
    }
}