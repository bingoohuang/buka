package kafka.api;

import com.google.common.collect.Maps;
import kafka.utils.Function2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;
import static kafka.api.ApiUtils.shortStringLength;

public class TopicData {
    public static TopicData readFrom(ByteBuffer buffer) {
        String topic = readShortString(buffer);
        int partitionCount = buffer.getInt();

        Map<Integer, FetchResponsePartitionData> topicPartitionData = Maps.newHashMap();

        for (int i = 1; i <= partitionCount; ++i) {
            int partitionId = buffer.getInt();
            FetchResponsePartitionData partitionData = FetchResponsePartitionData.readFrom(buffer);
            topicPartitionData.put(partitionId, partitionData);
        }

        return new TopicData(topic, topicPartitionData);
    }

    public static int headerSize(String topic) {
        return shortStringLength(topic) +
                4 /* partition count */;
    }


    public final String topic;
    public final Map<Integer, FetchResponsePartitionData> partitionData;


    public TopicData(String topic, Map<Integer, FetchResponsePartitionData> partitionData) {
        this.topic = topic;
        this.partitionData = partitionData;
    }

    public int sizeInBytes() {
        return TopicData.headerSize(topic) + Utils.foldLeft(partitionData.values(), 0,
                new Function2<Integer, FetchResponsePartitionData, Integer>() {
                    @Override
                    public Integer apply(Integer folded, FetchResponsePartitionData arg) {
                        return folded + arg.sizeInBytes() + 4;
                    }
                });
    }

    public int headerSize() {
        return TopicData.headerSize(topic);
    }
}
