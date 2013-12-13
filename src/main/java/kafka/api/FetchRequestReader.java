package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.utils.Function0;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;

public class FetchRequestReader implements RequestReader {
    public static final short CurrentVersion = 0;
    public static final int DefaultMaxWait = 0;
    public static final int DefaultMinBytes = 0;
    public static final int DefaultCorrelationId = 0;

    public static final RequestReader instance = new FetchRequestReader();

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int replicaId = buffer.getInt();
        int maxWait = buffer.getInt();
        int minBytes = buffer.getInt();
        int topicCount = buffer.getInt();

        Map<TopicAndPartition, PartitionFetchInfo> pairs = Utils.flatMaps(1, topicCount, new Function0<Map<TopicAndPartition, PartitionFetchInfo>>() {
            @Override
            public Map<TopicAndPartition, PartitionFetchInfo> apply() {
                final String topic = readShortString(buffer);
                int partitionCount = buffer.getInt();

                return Utils.map(1, partitionCount, new Function0<Tuple2<TopicAndPartition, PartitionFetchInfo>>() {
                    @Override
                    public Tuple2<TopicAndPartition, PartitionFetchInfo> apply() {
                        int partitionId = buffer.getInt();
                        long offset = buffer.getLong();
                        int fetchSize = buffer.getInt();
                        return Tuple2.make(new TopicAndPartition(topic, partitionId),
                                new PartitionFetchInfo(offset, fetchSize));
                    }
                });
            }
        });

        return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes, pairs);
    }
}
