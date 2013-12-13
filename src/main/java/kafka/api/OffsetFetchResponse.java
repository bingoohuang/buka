package kafka.api;

import com.google.common.collect.Table;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.*;

public class OffsetFetchResponse extends RequestOrResponse {
    public static final short CurrentVersion = 0;

    public static OffsetFetchResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
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
                        short error = buffer.getShort();

                        return Tuple2.make(new TopicAndPartition(topic, partitionId),
                                new OffsetMetadataAndError(offset, metadata, error));
                    }
                });
            }
        });

        return new OffsetFetchResponse(pairs, correlationId);
    }

    public Map<TopicAndPartition, OffsetMetadataAndError> requestInfo;

    public OffsetFetchResponse(Map<TopicAndPartition, OffsetMetadataAndError> requestInfo, int correlationId) {
        super(correlationId);
        this.requestInfo = requestInfo;

        requestInfoGroupedByTopic = Utils.groupBy(requestInfo, new Function2<TopicAndPartition, OffsetMetadataAndError, String>() {
            @Override
            public String apply(TopicAndPartition arg1, OffsetMetadataAndError arg2) {
                return arg1.topic;
            }
        });
    }

    public Table<String, TopicAndPartition, OffsetMetadataAndError> requestInfoGroupedByTopic;


    @Override
    public int sizeInBytes() {
        return 4 /* correlationId */
                + 4 /* topic count */
                + Utils.foldLeft(requestInfoGroupedByTopic, 0, new Function3<Integer, String, Map<TopicAndPartition, OffsetMetadataAndError>, Integer>() {
            @Override
            public Integer apply(Integer count, String topic, Map<TopicAndPartition, OffsetMetadataAndError> arg3) {
                return count
                        + shortStringLength(topic)  /* topic */
                        + 4  /* number of partitions */
                        + Utils.foldLeft(arg3, 0, new Function3<Integer, TopicAndPartition, OffsetMetadataAndError, Integer>() {
                    @Override
                    public Integer apply(Integer innerCount, TopicAndPartition arg2, OffsetMetadataAndError arg3) {
                        return innerCount + 4 /* partition */ +
                                8 /* offset */ +
                                shortStringLength(arg3.metadata) +
                                2 /* error */;
                    }
                });
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics

        Utils.foreach(requestInfoGroupedByTopic, new Callable2<String, Map<TopicAndPartition,OffsetMetadataAndError>>() {
            @Override
            public void apply(String topic, Map<TopicAndPartition, OffsetMetadataAndError> arg2) {
                writeShortString(buffer, topic); // topic
                buffer.putInt(arg2.size());       // number of partitions for this topic

                Utils.foreach(arg2, new Callable2<TopicAndPartition, OffsetMetadataAndError>() {
                    @Override
                    public void apply(TopicAndPartition _1, OffsetMetadataAndError _2) {
                        buffer.putInt(_1.partition);
                        buffer.putLong(_2.offset);
                        writeShortString(buffer, _2.metadata);
                        buffer.putShort(_2.error);
                    }
                });
            }
        });
    }
}
