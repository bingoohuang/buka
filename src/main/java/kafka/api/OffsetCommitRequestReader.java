package kafka.api;

import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.utils.Function0;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;

public class OffsetCommitRequestReader implements RequestReader {
    public static final RequestReader instance = new OffsetCommitRequestReader();

    public static short CurrentVersion = 0;
    public static String DefaultClientId = "";

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        // Read values from the envelope
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);

        // Read the OffsetRequest
        String consumerGroupId = readShortString(buffer);
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, OffsetMetadataAndError> pairs = Utils.flatMaps(1, topicCount, new Function0<Map<TopicAndPartition, OffsetMetadataAndError>>() {
            @Override
            public Map<TopicAndPartition, OffsetMetadataAndError> apply() {
                final String topic = readShortString(buffer);
                int partitionCount = buffer.getInt();

                return Utils.flatMap(1, partitionCount, new Function0<Tuple2<TopicAndPartition, OffsetMetadataAndError>>() {
                    @Override
                    public Tuple2<TopicAndPartition, OffsetMetadataAndError> apply() {
                        int partitionId = buffer.getInt();
                        long offset = buffer.getLong();
                        String metadata = readShortString(buffer);

                        return Tuple2.make(
                                new TopicAndPartition(topic, partitionId),
                                new OffsetMetadataAndError(offset, metadata)
                        );
                    }
                });
            }
        });

        return new OffsetCommitRequest(consumerGroupId, pairs, versionId, correlationId, clientId);
    }
}
