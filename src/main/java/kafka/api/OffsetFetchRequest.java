package kafka.api;

import com.google.common.collect.Multimap;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;

public class OffsetFetchRequest extends RequestOrResponse {
    public String groupId;
    public List<TopicAndPartition> requestInfo;
    public short versionId;
    public String clientId;

    public OffsetFetchRequest(String groupId,
                              List<TopicAndPartition> requestInfo) {
        this(groupId, requestInfo, OffsetFetchRequestReader.CurrentVersion, 0, OffsetFetchRequestReader.DefaultClientId);
    }

    public OffsetFetchRequest(String groupId,
                              List<TopicAndPartition> requestInfo,
                              short versionId,
                              int correlationId,
                              String clientId) {
        super(RequestKeys.OffsetFetchKey, correlationId);
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;

        requestInfoGroupedByTopic = Utils.groupBy(requestInfo, new Function1<TopicAndPartition, Tuple2<String, TopicAndPartition>>() {
            @Override
            public Tuple2<String, TopicAndPartition> apply(TopicAndPartition topicAndPartition) {
                return Tuple2.make(topicAndPartition.topic, topicAndPartition);
            }
        });
    }

    private Multimap<String, TopicAndPartition> requestInfoGroupedByTopic;

    @Override
    public int sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlationId */
                shortStringLength(clientId) +
                shortStringLength(groupId) +
                4  /* topic count */
                + Utils.foldLeft(requestInfoGroupedByTopic, 0,
                new Function3<Integer, String, Collection<TopicAndPartition>, Integer>() {
                    @Override
                    public Integer apply(Integer count, String topic, Collection<TopicAndPartition> arg3) {
                        return count + shortStringLength(topic) + /* topic */
                                4 + /* number of partitions */
                                arg3.size() * 4 /* partition */;
                    }
                });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        // Write envelope
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);

        // Write OffsetFetchRequest
        writeShortString(buffer, groupId);             // consumer group
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics

        Utils.foreach(requestInfoGroupedByTopic, new Function2<String, Collection<TopicAndPartition>, Void>() {
            @Override
            public Void apply(String topic, Collection<TopicAndPartition> topicAndPartitions) {
                writeShortString(buffer, topic); // topic
                buffer.putInt(topicAndPartitions.size());       // number of partitions for this topic

                Utils.foreach(topicAndPartitions, new Function1<TopicAndPartition, Void>() {
                    @Override
                    public Void apply(TopicAndPartition arg) {
                        buffer.putInt(arg.partition);
                        return null;
                    }
                });

                return null;
            }
        });
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<TopicAndPartition, OffsetMetadataAndError> responseMap = Utils.map(requestInfo, new Function1<TopicAndPartition, Tuple2<TopicAndPartition, OffsetMetadataAndError>>() {
            @Override
            public Tuple2<TopicAndPartition, OffsetMetadataAndError> apply(TopicAndPartition topicAndPartition) {
                return Tuple2.make(topicAndPartition,
                        new OffsetMetadataAndError(OffsetMetadataAndError.InvalidOffset,
                                OffsetMetadataAndError.NoMetadata,
                                ErrorMapping.codeFor(e.getClass())));
            }
        });

        OffsetFetchResponse errorResponse = new OffsetFetchResponse(responseMap, correlationId);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
