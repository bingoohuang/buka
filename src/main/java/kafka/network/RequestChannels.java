package kafka.network;

import com.google.common.collect.Maps;
import kafka.api.ProducerRequest;
import kafka.api.RequestKeys;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;

import java.nio.ByteBuffer;

public class RequestChannels {
    public static final Request AllDone = new Request(1, 2, getShutdownReceive(), 0);

    public static ByteBuffer getShutdownReceive() {
        ProducerRequest emptyProducerRequest = new ProducerRequest((short) 0, 0, "", (short) 0, 0,
                Maps.<TopicAndPartition, ByteBufferMessageSet>newHashMap());

        ByteBuffer byteBuffer = ByteBuffer.allocate(emptyProducerRequest.sizeInBytes() + 2);
        byteBuffer.putShort(RequestKeys.ProduceKey);
        emptyProducerRequest.writeTo(byteBuffer);
        byteBuffer.rewind();
        return byteBuffer;
    }

}
