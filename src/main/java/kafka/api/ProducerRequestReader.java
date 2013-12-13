package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.utils.Function0;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;

public class ProducerRequestReader implements RequestReader {
    public static final RequestReader instance = new ProducerRequestReader();

    public final static short CurrentVersion = 0;


    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        final short versionId = buffer.getShort();
        final int correlationId = buffer.getInt();
        final String clientId = readShortString(buffer);
        final short requiredAcks = buffer.getShort();
        final int ackTimeoutMs = buffer.getInt();
        //build the topic structure
        int topicCount = buffer.getInt();

        final Map<TopicAndPartition, ByteBufferMessageSet> partitionData =
                Utils.flatMaps(1, topicCount, new Function0<Map<TopicAndPartition, ByteBufferMessageSet>>() {

                    @Override
                    public Map<TopicAndPartition, ByteBufferMessageSet> apply() {
                        // process topic
                        final String topic = readShortString(buffer);
                        int partitionCount = buffer.getInt();

                        return Utils.map(1, partitionCount, new Function0<Tuple2<TopicAndPartition, ByteBufferMessageSet>>() {

                            @Override
                            public Tuple2<TopicAndPartition, ByteBufferMessageSet> apply() {
                                int partition = buffer.getInt();
                                int messageSetSize = buffer.getInt();
                                byte[] messageSetBuffer = new byte[messageSetSize];
                                buffer.get(messageSetBuffer, 0, messageSetSize);

                                return Tuple2.make(new TopicAndPartition(topic, partition),
                                        new ByteBufferMessageSet(ByteBuffer.wrap(messageSetBuffer)));
                            }
                        });
                    }
                });

        return new ProducerRequest(versionId, correlationId, clientId, requiredAcks, ackTimeoutMs, partitionData);
    }

}
