package kafka.api;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import kafka.common.TopicAndPartition;
import kafka.utils.Function0;
import kafka.utils.Function2;
import kafka.utils.Function3;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.*;

public class OffsetCommitResponse extends RequestOrResponse {
    public static final short CurrentVersion = 0;

    public static OffsetCommitResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();


        Map<TopicAndPartition, Short> pairs = Utils.flatMap(1, topicCount, new Function0<Map<TopicAndPartition, Short>>() {
            @Override
            public Map<TopicAndPartition, Short> apply() {
                final String topic = readShortString(buffer);
                int partitionCount = buffer.getInt();

                return Utils.flatMap(1, partitionCount, new Function0<Map<TopicAndPartition, Short>>() {
                    @Override
                    public Map<TopicAndPartition, Short> apply() {
                        int partitionId = buffer.getInt();
                        short error = buffer.getShort();
                        return ImmutableMap.of(new TopicAndPartition(topic, partitionId), error);
                    }
                });
            }
        });

        return new OffsetCommitResponse(pairs, correlationId);
    }


    public Map<TopicAndPartition, Short> requestInfo;

    public OffsetCommitResponse(Map<TopicAndPartition, Short> requestInfo) {
        this(requestInfo, 0);
    }

    public OffsetCommitResponse(Map<TopicAndPartition, Short> requestInfo, int correlationId) {
        super(correlationId);
        this.requestInfo = requestInfo;

        requestInfoGroupedByTopic = Utils.groupBy(requestInfo, new Function2<TopicAndPartition, Short, String>() {
            @Override
            public String apply(TopicAndPartition arg1, Short arg2) {
                return arg1.topic;
            }
        });
    }

    public Table<String, TopicAndPartition, Short> requestInfoGroupedByTopic;

    @Override
    public int sizeInBytes() {
        return 4  /* correlationId */
                + 4  /* topic count */
                + Utils.foldLeft(requestInfoGroupedByTopic, 0, new Function3<Integer, String, Map<TopicAndPartition, Short>, Integer>() {
            @Override
            public Integer apply(Integer count, String topic, Map<TopicAndPartition, Short> offsets) {
                return count + shortStringLength(topic) + /* topic */
                        4 + /* number of partitions */
                        offsets.size() * (
                                4 + /* partition */
                                        2 /* error */
                        );
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics

        Utils.foreach(requestInfoGroupedByTopic, new Function2<String, Map<TopicAndPartition, Short>, Void>() {
            @Override
            public Void apply(String topic, Map<TopicAndPartition, Short> arg2) {
                writeShortString(buffer, topic); // topic
                buffer.putInt(arg2.size());       // number of partitions for this topic

                Utils.foreach(arg2, new Function2<TopicAndPartition, Short, Void>() {
                    @Override
                    public Void apply(TopicAndPartition arg1, Short arg2) {
                        buffer.putInt(arg1.partition);
                        buffer.putShort(arg2);  //error

                        return null;
                    }
                });

                return null;
            }
        });
    }
}
