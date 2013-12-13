package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.*;

public class OffsetResponse extends RequestOrResponse {
    public static OffsetResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int numTopics = buffer.getInt();
        Map<TopicAndPartition, PartitionOffsetsResponse> pairs = Utils.flatMaps(1, numTopics, new Function0<Map<TopicAndPartition, PartitionOffsetsResponse>>() {
            @Override
            public Map<TopicAndPartition, PartitionOffsetsResponse> apply() {
                final String topic = readShortString(buffer);
                int numPartitions = buffer.getInt();
                return Utils.flatMap(1, numPartitions, new Function0<Tuple2<TopicAndPartition, PartitionOffsetsResponse>>() {
                    @Override
                    public Tuple2<TopicAndPartition, PartitionOffsetsResponse> apply() {
                        int partition = buffer.getInt();
                        short error = buffer.getShort();
                        int numOffsets = buffer.getInt();

                        List<Long> offsets = Utils.flatList(1, numOffsets, new Function1<Integer, Long>() {
                            @Override
                            public Long apply(Integer arg) {
                                return buffer.getLong();
                            }
                        });

                        return Tuple2.make(new TopicAndPartition(topic, partition),
                                new PartitionOffsetsResponse(error, offsets));
                    }
                });
            }
        });

        return new OffsetResponse(correlationId, pairs);
    }

    public Map<TopicAndPartition, PartitionOffsetsResponse> partitionErrorAndOffsets;

    public OffsetResponse(int correlationId,
                          Map<TopicAndPartition, PartitionOffsetsResponse> partitionOffsetResponseMap) {
        super(correlationId);
        this.partitionErrorAndOffsets = partitionOffsetResponseMap;

        offsetsGroupedByTopic = Utils.groupBy(partitionErrorAndOffsets, new Function2<TopicAndPartition, PartitionOffsetsResponse, String>() {
            @Override
            public String apply(TopicAndPartition arg1, PartitionOffsetsResponse arg2) {
                return arg1.topic;
            }
        });
    }

    Table<String, TopicAndPartition, PartitionOffsetsResponse> offsetsGroupedByTopic;

    public boolean hasError() {
        return Utils.exists(partitionErrorAndOffsets.values(), new Function1<PartitionOffsetsResponse, Boolean>() {
            @Override
            public Boolean apply(PartitionOffsetsResponse arg) {
                return arg.error != ErrorMapping.NoError;
            }
        });
    }

    @Override
    public int sizeInBytes() {
        return 4 + /* correlation id */
                4  /* topic count */
                + Utils.foldLeft(offsetsGroupedByTopic, 0, new Function3<Integer, String, Map<TopicAndPartition, PartitionOffsetsResponse>, Integer>() {
            @Override
            public Integer apply(Integer foldedTopics, String topic, Map<TopicAndPartition, PartitionOffsetsResponse> errorAndOffsetsMap) {
                return foldedTopics + shortStringLength(topic) +
                        4  /* partition count */
                        + Utils.foldLeft(errorAndOffsetsMap, 0, new Function3<Integer, TopicAndPartition, PartitionOffsetsResponse, Integer>() {
                    @Override
                    public Integer apply(Integer foldedPartitions, TopicAndPartition arg2, PartitionOffsetsResponse arg3) {
                        return foldedPartitions +
                                4 + /* partition id */
                                2 + /* partition error */
                                4 + /* offset array length */
                                arg3.offsets.size() * 8 /* offset */
                                ;
                    }
                });
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(offsetsGroupedByTopic.size()); // topic count

        Utils.foreach(offsetsGroupedByTopic, new Function2<String, Map<TopicAndPartition, PartitionOffsetsResponse>, Void>() {
            @Override
            public Void apply(String topic, Map<TopicAndPartition, PartitionOffsetsResponse> errorAndOffsetsMap) {
                writeShortString(buffer, topic);
                buffer.putInt(errorAndOffsetsMap.size()); // partition count

                Utils.foreach(errorAndOffsetsMap, new Function2<TopicAndPartition, PartitionOffsetsResponse, Void>() {
                    @Override
                    public Void apply(TopicAndPartition arg1, PartitionOffsetsResponse errorAndOffsets) {
                        buffer.putInt(arg1.partition);
                        buffer.putShort(errorAndOffsets.error);
                        buffer.putInt(errorAndOffsets.offsets.size()); // offset array length

                        Utils.foreach(errorAndOffsets.offsets, new Function1<Long, Void>() {
                            @Override
                            public Void apply(Long arg) {
                                buffer.putLong(arg);
                                return null;
                            }
                        });

                        return null;
                    }
                });
                return null;
            }
        });
    }
}
