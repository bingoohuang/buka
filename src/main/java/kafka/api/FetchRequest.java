package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.consumer.ConsumerConfigs;
import kafka.message.MessageSets;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.Function2;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.shortStringLength;
import static kafka.api.ApiUtils.writeShortString;

public class FetchRequest extends RequestOrResponse {
    public short versionId /*= FetchRequestReader.CurrentVersion*/;
    //  override val correlationId: Int = FetchRequest.DefaultCorrelationId,
    public String clientId;/* = ConsumerConfig.DefaultClientId,*/
    public int replicaId; /*Request.OrdinaryConsumerId,*/
    public int maxWait; /* FetchRequest.DefaultMaxWait,*/
    public int minBytes; /*FetchRequest.DefaultMinBytes,*/
    public Map<TopicAndPartition, PartitionFetchInfo> requestInfo;


    public FetchRequest(Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this(FetchRequestReader.CurrentVersion,
                FetchRequestReader.DefaultCorrelationId,
                ConsumerConfigs.DefaultClientId,
                Requests.OrdinaryConsumerId,
                FetchRequestReader.DefaultMaxWait,
                FetchRequestReader.DefaultMinBytes,
                requestInfo
        );
    }

    public FetchRequest(short versionId,
                        int correlationId,
                        String clientId,
                        int replicaId,
                        int maxWait,
                        int minBytes,
                        Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        super(RequestKeys.FetchKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.replicaId = replicaId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.requestInfo = requestInfo;

        // = requestInfo.groupBy(_._1.topic)
        requestInfoGroupedByTopic = Utils.groupBy(requestInfo, new Function2<TopicAndPartition, PartitionFetchInfo, String>() {
            @Override
            public String apply(TopicAndPartition arg1, PartitionFetchInfo arg2) {
                return arg1.topic;
            }
        });
    }

    /**
     * Partitions the request info into a map of maps (one for each topic).
     */
    Table<String, TopicAndPartition, PartitionFetchInfo> requestInfoGroupedByTopic;

    /**
     * Public constructor for the clients
     */
    public FetchRequest(int correlationId,
                        String clientId,
                        int maxWait,
                        int minBytes,
                        Map<TopicAndPartition, PartitionFetchInfo> requestInfo) {
        this(FetchRequestReader.CurrentVersion,
                correlationId,
                clientId,
                Requests.OrdinaryConsumerId,
                maxWait,
                minBytes,
                requestInfo);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(replicaId);
        buffer.putInt(maxWait);
        buffer.putInt(minBytes);
        buffer.putInt(requestInfoGroupedByTopic.size()); // topic count

        for (String topic : requestInfoGroupedByTopic.rowKeySet()) {
            Map<TopicAndPartition, PartitionFetchInfo> partitionFetchInfos = requestInfoGroupedByTopic.row(topic);

            writeShortString(buffer, topic);
            buffer.putInt(partitionFetchInfos.size()); // partition count

            for (Map.Entry<TopicAndPartition, PartitionFetchInfo> currPartition : partitionFetchInfos.entrySet()) {
                int partition = currPartition.getKey().partition;
                long offset = currPartition.getValue().offset;
                int fetchSize = currPartition.getValue().fetchSize;
                buffer.putInt(partition);
                buffer.putLong(offset);
                buffer.putInt(fetchSize);
            }
        }
    }

    public int sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlationId */
                shortStringLength(clientId) +
                4 + /* replicaId */
                4 + /* maxWait */
                4 + /* minBytes */
                4 + /* topic count */
                +compute();
    }

    private int compute() {
        int total = 0;
        for (String topic : requestInfoGroupedByTopic.rowKeySet()) {
            Map<TopicAndPartition, PartitionFetchInfo>
                    partitionFetchInfos = requestInfoGroupedByTopic.row(topic);
            total += shortStringLength(topic);
            total += 4;  /* partition count */
            total += partitionFetchInfos.size() * (
                    4 + /* partition id */
                            8 + /* offset */
                            4 /* fetch size */
            );
        }
        return total;
    }

    public boolean isFromFollower() {
        return Requests.isReplicaIdFromFollower(replicaId);
    }

    public boolean isFromOrdinaryConsumer() {
        return replicaId == Requests.OrdinaryConsumerId;
    }

    public boolean isFromLowLevelConsumer() {
        return replicaId == Requests.DebuggingConsumerId;
    }

    public int numPartitions() {
        return requestInfo.size();
    }

    @Override
    public String toString() {
        StringBuilder fetchRequest = new StringBuilder();
        fetchRequest.append("Name: " + this.getClass().getSimpleName());
        fetchRequest.append("; Version: " + versionId);
        fetchRequest.append("; CorrelationId: " + correlationId);
        fetchRequest.append("; ClientId: " + clientId);
        fetchRequest.append("; ReplicaId: " + replicaId);
        fetchRequest.append("; MaxWait: " + maxWait + " ms");
        fetchRequest.append("; MinBytes: " + minBytes + " bytes");
        fetchRequest.append("; RequestInfo: " + requestInfo);
        return fetchRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<TopicAndPartition, FetchResponsePartitionData>
                fetchResponsePartitionData = Utils.map(requestInfo,
                new Function2<TopicAndPartition, PartitionFetchInfo, Tuple2<TopicAndPartition, FetchResponsePartitionData>>() {
                    @Override
                    public Tuple2<TopicAndPartition, FetchResponsePartitionData> apply(TopicAndPartition arg1, PartitionFetchInfo arg2) {
                        return Tuple2.make(arg1, new FetchResponsePartitionData(ErrorMapping.codeFor(e.getClass()), -1, MessageSets.Empty));
                    }
                });

        FetchResponse errorResponse = new FetchResponse(correlationId, fetchResponsePartitionData);
        requestChannel.sendResponse(new Response(request, new FetchResponseSend(errorResponse)));
    }
}
