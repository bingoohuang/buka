package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.Function1;
import kafka.utils.Function2;
import kafka.utils.Function3;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static kafka.api.ApiUtils.writeShortString;

public class UpdateMetadataRequest extends RequestOrResponse {
    public short versionId;
    public String clientId;
    public int controllerId;
    public int controllerEpoch;
    public Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos;
    public Set<Broker> aliveBrokers;

    public UpdateMetadataRequest(short versionId,
                                 int correlationId,
                                 String clientId,
                                 int controllerId,
                                 int controllerEpoch,
                                 Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos,
                                 Set<Broker> aliveBrokers) {
        super(RequestKeys.UpdateMetadataKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.aliveBrokers = aliveBrokers;
    }

    public UpdateMetadataRequest(int controllerId, int controllerEpoch, int correlationId, String clientId,
                                 Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos,
                                 Set<Broker> aliveBrokers) {
        this(UpdateMetadataRequestReader.CurrentVersion, correlationId, clientId,
                controllerId, controllerEpoch, partitionStateInfos, aliveBrokers);
    }

    @Override
    public int sizeInBytes() {
        return 2 /* version id */ +
                4 /* correlation id */ +
                (2 + clientId.length()) /* client id */ +
                4 /* controller id */ +
                4 /* controller epoch */ +
                4 /* number of partitions */
                + Utils.foldLeft(partitionStateInfos, 0, new Function3<Integer, TopicAndPartition, PartitionStateInfo, Integer>() {
            @Override
            public Integer apply(Integer arg1, TopicAndPartition key, PartitionStateInfo value) {
                return arg1 + (2 + key.topic.length())
                         /* topic */ + 4 /* partition */ + value.sizeInBytes() /* partition state info */
                        + 4 /* number of alive brokers in the cluster */;
            }
        }) + Utils.foldLeft(aliveBrokers, 0, new Function2<Integer, Broker, Integer>() {
            @Override
            public Integer apply(Integer arg1, Broker _) {
                return arg1 + _.sizeInBytes();
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putShort(versionId);
        buffer.putInt(correlationId);
        writeShortString(buffer, clientId);
        buffer.putInt(controllerId);
        buffer.putInt(controllerEpoch);
        buffer.putInt(partitionStateInfos.size());
        Utils.foreach(partitionStateInfos, new Function2<TopicAndPartition, PartitionStateInfo, Void>() {
            @Override
            public Void apply(TopicAndPartition key, PartitionStateInfo value) {
                writeShortString(buffer, key.topic);
                buffer.putInt(key.partition);
                value.writeTo(buffer);

                return null;
            }
        });

        buffer.putInt(aliveBrokers.size());

        Utils.foreach(aliveBrokers, new Function1<Broker, Void>() {
            @Override
            public Void apply(Broker _) {
                _.writeTo(buffer);
                return null;
            }
        });
    }

    @Override
    public void handleError(Throwable e, RequestChannel requestChannel, Request request) {
        UpdateMetadataResponse errorResponse = new UpdateMetadataResponse(correlationId, ErrorMapping.codeFor(e.getClass()));
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
