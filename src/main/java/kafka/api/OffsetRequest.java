package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;

public class OffsetRequest extends RequestOrResponse {
    public Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo;
    public short versionId;
    public String clientId;
    public int replicaId;

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo) {
        this(requestInfo, OffsetRequestReader.CurrentVersion, 0,
                OffsetRequestReader.DefaultClientId, Requests.OrdinaryConsumerId);
    }

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo,
                         short versionId,
                         int correlationId,
                         String clientId,
                         int replicaId) {
        super(RequestKeys.OffsetsKey, correlationId);
        this.requestInfo = requestInfo;
        this.versionId = versionId;
        this.clientId = clientId;
        this.replicaId = replicaId;

        requestInfoGroupedByTopic = Utils.groupBy(requestInfo, new Function2<TopicAndPartition, PartitionOffsetRequestInfo, String>() {
            @Override
            public String apply(TopicAndPartition arg1, PartitionOffsetRequestInfo arg2) {
                return arg1.topic;
            }
        });
    }

    public OffsetRequest(Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo,
                         int correlationId,
                         int replicaId) {
        this(requestInfo, OffsetRequestReader.CurrentVersion, correlationId,
                OffsetRequestReader.DefaultClientId, replicaId);
    }

    Table<String, TopicAndPartition, PartitionOffsetRequestInfo> requestInfoGroupedByTopic;

    @Override
    public int sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlationId */
                shortStringLength(clientId) +
                4 + /* replicaId */
                4  /* topic count */
                + Utils.foldLeft(requestInfoGroupedByTopic, 0, new Function3<Integer, String, Map<TopicAndPartition, PartitionOffsetRequestInfo>, Integer>() {
            @Override
            public Integer apply(Integer foldedTopics, String topic, Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionInfos) {
                return foldedTopics + shortStringLength(topic) +
                        4 + /* partition count */
                        partitionInfos.size() * (
                                4 + /* partition */
                                        8 + /* time */
                                        4 /* maxNumOffsets */
                        );
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(replicaId);

        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count

        Utils.foreach(requestInfoGroupedByTopic, new Callable2<String, Map<TopicAndPartition,PartitionOffsetRequestInfo>>() {
            @Override
            public void apply(String topic, Map<TopicAndPartition, PartitionOffsetRequestInfo> partitionInfos) {
                writeShortString(buffer, topic);
                buffer.putInt(partitionInfos.size()); // partition count

                Utils.foreach(partitionInfos, new Callable2<TopicAndPartition, PartitionOffsetRequestInfo>() {
                    @Override
                    public void apply(TopicAndPartition arg1, PartitionOffsetRequestInfo partitionInfo) {
                        buffer.putInt(arg1.partition);
                        buffer.putLong(partitionInfo.time);
                        buffer.putInt(partitionInfo.maxNumOffsets);
                    }
                });
            }
        });
    }

    public boolean isFromOrdinaryClient() {
        return replicaId == Requests.OrdinaryConsumerId;
    }

    public boolean isFromDebuggingClient() {
        return replicaId == Requests.DebuggingConsumerId;
    }

    @Override
    public String toString() {
        StringBuilder offsetRequest = new StringBuilder();
        offsetRequest.append("Name: " + this.getClass().getSimpleName());
        offsetRequest.append("; Version: " + versionId);
        offsetRequest.append("; CorrelationId: " + correlationId);
        offsetRequest.append("; ClientId: " + clientId);
        offsetRequest.append("; RequestInfo: " + requestInfo);
        offsetRequest.append("; ReplicaId: " + replicaId);
        return offsetRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<TopicAndPartition, PartitionOffsetsResponse> partitionOffsetResponseMap = Utils.map(requestInfo, new Function2<TopicAndPartition, PartitionOffsetRequestInfo, Tuple2<TopicAndPartition, PartitionOffsetsResponse>>() {
            @Override
            public Tuple2<TopicAndPartition, PartitionOffsetsResponse> apply(TopicAndPartition topicAndPartition, PartitionOffsetRequestInfo partitionOffsetRequest) {
                return Tuple2.make(topicAndPartition, new PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass()), null));
            }
        });

        OffsetResponse errorResponse = new OffsetResponse(correlationId, partitionOffsetResponseMap);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
