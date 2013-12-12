package kafka.api;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;

public class FetchResponse {
    public static final int headerSize =
            4 + /* correlationId */
                    4 /* topic count */;

    public static FetchResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();

        Map<TopicAndPartition, FetchResponsePartitionData> data = Maps.newHashMap();
        for (int i = 0; i <= topicCount; ++i) {
            TopicData topicData = TopicData.readFrom(buffer);
            for (Map.Entry<Integer, FetchResponsePartitionData> entry : topicData.partitionData.entrySet()) {
                int partitionId = entry.getKey();
                FetchResponsePartitionData partitionData = entry.getValue();
                data.put(new TopicAndPartition(topicData.topic, partitionId), partitionData);
            }
        }

        return new FetchResponse(correlationId, data);
    }

    public int correlationId;
    public Map<TopicAndPartition, FetchResponsePartitionData> data;

    public FetchResponse(int correlationId, Map<TopicAndPartition, FetchResponsePartitionData> data) {
        this.correlationId = correlationId;
        this.data = data;

        dataGroupedByTopic = Utils.groupBy(data, new Function2<TopicAndPartition, FetchResponsePartitionData, String>() {
            @Override
            public String apply(TopicAndPartition arg1, FetchResponsePartitionData arg2) {
                return arg1.topic;
            }
        });
    }

    /**
     * Partitions the data into a map of maps (one for each topic).
     */
    Table<String, TopicAndPartition, FetchResponsePartitionData> dataGroupedByTopic;

    public int sizeInBytes() {
        return FetchResponse.headerSize +
                Utils.foldLeft(dataGroupedByTopic, 0,
                        new Function3<Integer, String, Map<TopicAndPartition, FetchResponsePartitionData>, Integer>() {
                            @Override
                            public Integer apply(Integer folded, String topic, Map<TopicAndPartition, FetchResponsePartitionData> arg3) {
                                TopicData topicData = new TopicData(topic, Utils.map(arg3, new Function2<TopicAndPartition, FetchResponsePartitionData, Tuple2<Integer, FetchResponsePartitionData>>() {
                                    @Override
                                    public Tuple2<Integer, FetchResponsePartitionData> apply(TopicAndPartition arg1, FetchResponsePartitionData arg2) {
                                        return Tuple2.make(arg1.partition, arg2);
                                    }
                                }));
                                return folded + topicData.sizeInBytes();
                            }
                        });
    }

    private FetchResponsePartitionData partitionDataFor(String topic, int partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        FetchResponsePartitionData fetchResponsePartitionData = data.get(topicAndPartition);
        if (fetchResponsePartitionData != null) return fetchResponsePartitionData;


        throw new IllegalArgumentException(
                String.format("No partition %s in fetch response %s", topicAndPartition, this.toString()));
    }

    public ByteBufferMessageSet messageSet(String topic, int partition) {
        return (ByteBufferMessageSet) partitionDataFor(topic, partition).messages;
    }

    public long highWatermark(String topic, int partition) {
        return partitionDataFor(topic, partition).hw;
    }

    public boolean hasError() {
        return Utils.exists(data.values(), new Function1<FetchResponsePartitionData, Boolean>() {
            @Override
            public Boolean apply(FetchResponsePartitionData arg) {
                return arg.error != ErrorMapping.NoError;
            }
        });
    }

    public int errorCode(String topic, int partition) {
        return partitionDataFor(topic, partition).error;
    }
}
