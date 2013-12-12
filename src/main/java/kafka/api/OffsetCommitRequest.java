package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.Function2;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.writeShortString;

public class OffsetCommitRequest extends RequestOrResponse {
    public String groupId;
    public Map<TopicAndPartition, OffsetMetadataAndError> requestInfo;
    public short versionId;
    public String clientId;

    public OffsetCommitRequest(String groupId,
                               Map<TopicAndPartition, OffsetMetadataAndError> requestInfo,
                               short versionId,
                               int correlationId,
                               String clientId) {
        super(RequestKeys.OffsetCommitKey, correlationId);
        this.groupId = groupId;
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;

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
        return 0;
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        // Write envelope
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);

        // Write OffsetCommitRequest
        writeShortString(buffer, groupId);             // consumer group
        buffer.putInt(requestInfoGroupedByTopic.size()); // number of topics
        Utils.foreach(requestInfoGroupedByTopic, new Function2<String, Map<TopicAndPartition, OffsetMetadataAndError>, Void>() {
            @Override
            public Void apply(String topic, Map<TopicAndPartition, OffsetMetadataAndError> arg2) {
                writeShortString(buffer, topic); // topic
                buffer.putInt(arg2.size());       // number of partitions for this topic

                Utils.foreach(arg2, new Function2<TopicAndPartition, OffsetMetadataAndError, Void>() {
                    @Override
                    public Void apply(TopicAndPartition a1, OffsetMetadataAndError a2) {
                        buffer.putInt(a1.partition);  // partition
                        buffer.putLong(a2.offset);    // offset
                        writeShortString(buffer, a2.metadata); // metadata
                        return null;
                    }
                });

                return null;
            }
        });
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<TopicAndPartition, Short> responseMap = Utils.map(requestInfo, new Function2<TopicAndPartition, OffsetMetadataAndError, Tuple2<TopicAndPartition, Short>>() {
            @Override
            public Tuple2<TopicAndPartition, Short> apply(TopicAndPartition topicAndPartition, OffsetMetadataAndError offset) {
                return Tuple2.make(topicAndPartition, ErrorMapping.codeFor(e.getClass()));
            }
        });

        OffsetCommitResponse errorResponse = new OffsetCommitResponse(responseMap, correlationId);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
