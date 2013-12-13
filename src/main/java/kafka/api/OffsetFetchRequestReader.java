package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.utils.Function1;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;

import static kafka.api.ApiUtils.readShortString;

public class OffsetFetchRequestReader implements RequestReader {
    public static final RequestReader instance = new OffsetFetchRequestReader();

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        // Read values from the envelope
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);

        // Read the OffsetFetchRequest
        String consumerGroupId = readShortString(buffer);
        int topicCount = buffer.getInt();
        List<TopicAndPartition> pairs = Utils.flatLists(1, topicCount, new Function1<Integer, List<TopicAndPartition>>() {
            @Override
            public List<TopicAndPartition> apply(Integer arg) {
                final String topic = readShortString(buffer);
                int partitionCount = buffer.getInt();

                return Utils.flatList(1, partitionCount, new Function1<Integer, TopicAndPartition>() {
                    @Override
                    public TopicAndPartition apply(Integer arg) {
                        int partitionId = buffer.getInt();
                        return new TopicAndPartition(topic, partitionId);
                    }
                });
            }
        });

        return new OffsetFetchRequest(consumerGroupId, pairs, versionId, correlationId, clientId);
    }
}
