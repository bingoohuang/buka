package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.utils.Function0;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;

public class OffsetRequestReader implements RequestReader {
    public static final RequestReader instance = new OffsetRequestReader();

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int replicaId = buffer.getInt();
        int topicCount = buffer.getInt();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> pairs = Utils.flatMaps(1, topicCount, new Function0<Map<TopicAndPartition, PartitionOffsetRequestInfo>>() {
            @Override
            public Map<TopicAndPartition, PartitionOffsetRequestInfo> apply() {
                final String topic = readShortString(buffer);
                int partitionCount = buffer.getInt();

                return Utils.flatMap(1, partitionCount, new Function0<Tuple2<TopicAndPartition, PartitionOffsetRequestInfo>>() {
                    @Override
                    public Tuple2<TopicAndPartition, PartitionOffsetRequestInfo> apply() {
                        int partitionId = buffer.getInt();
                        long time = buffer.getLong();
                        int maxNumOffsets = buffer.getInt();

                        return Tuple2.make(
                                new TopicAndPartition(topic, partitionId),
                                new PartitionOffsetRequestInfo(time, maxNumOffsets)
                        );
                    }
                });
            }
        });


        return new OffsetRequest(pairs, versionId, correlationId, clientId, replicaId);

    }
}
