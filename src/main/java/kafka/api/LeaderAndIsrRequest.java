package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Request;
import kafka.network.RequestChannel;
import kafka.network.Response;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static kafka.api.ApiUtils.writeShortString;

public class LeaderAndIsrRequest extends RequestOrResponse {
    public short versionId;
    public String clientId;
    public int controllerId;
    public int controllerEpoch;
    public Map<Tuple2<String, Integer>, PartitionStateInfo> partitionStateInfos;
    public Set<Broker> leaders;

    public LeaderAndIsrRequest(short versionId,
                               int correlationId,
                               String clientId,
                               int controllerId,
                               int controllerEpoch,
                               Map<Tuple2<String, Integer>, PartitionStateInfo> partitionStateInfos,
                               Set<Broker> leaders) {
        super(RequestKeys.LeaderAndIsrKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.partitionStateInfos = partitionStateInfos;
        this.leaders = leaders;
    }

    public LeaderAndIsrRequest(Map<Tuple2<String, Integer>, PartitionStateInfo> partitionStateInfos,
                               Set<Broker> leaders,
                               int controllerId,
                               int controllerEpoch,
                               int correlationId,
                               String clientId) {
        this(LeaderAndIsrRequestReader.CurrentVersion, correlationId, clientId,
                controllerId, controllerEpoch, partitionStateInfos, leaders);
    }


    @Override
    public int sizeInBytes() {
        return 2 /* version id */ +
                4 /* correlation id */ +
                (2 + clientId.length()) /* client id */ +
                4 /* controller id */ +
                4 /* controller epoch */ +
                4 /* number of partitions */ +
                Utils.foldLeft(partitionStateInfos, 0, new Function3<Integer, Tuple2<String, Integer>, PartitionStateInfo, Integer>() {
                    @Override
                    public Integer apply(Integer arg1, Tuple2<String, Integer> key, PartitionStateInfo value) {
                        return arg1 + (2 + key._1.length()) /* topic */
                                + 4 /* partition */
                                + value.sizeInBytes() /* partition state info */;
                    }
                })
                + 4 /* number of leader brokers */
                + Utils.foldLeft(leaders, 0, new Function2<Integer, Broker, Integer>() {
            @Override
            public Integer apply(Integer arg1, Broker broker) {
                return arg1 + broker.sizeInBytes(); /* broker info */
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

        Utils.foreach(partitionStateInfos, new Function2<Tuple2<String, Integer>, PartitionStateInfo, Void>() {
            @Override
            public Void apply(Tuple2<String, Integer> key, PartitionStateInfo value) {
                writeShortString(buffer, key._1);
                buffer.putInt(key._2);
                value.writeTo(buffer);

                return null;
            }
        });

        buffer.putInt(leaders.size());

        Utils.foreach(leaders, new Function1<Broker, Void>() {
            @Override
            public Void apply(Broker broker) {
                broker.writeTo(buffer);
                return null;
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder leaderAndIsrRequest = new StringBuilder();
        leaderAndIsrRequest.append("Name:" + this.getClass().getSimpleName());
        leaderAndIsrRequest.append(";Version:" + versionId);
        leaderAndIsrRequest.append(";Controller:" + controllerId);
        leaderAndIsrRequest.append(";ControllerEpoch:" + controllerEpoch);
        leaderAndIsrRequest.append(";CorrelationId:" + correlationId);
        leaderAndIsrRequest.append(";ClientId:" + clientId);
        leaderAndIsrRequest.append(";PartitionState:" + partitionStateInfos);
        leaderAndIsrRequest.append(";Leaders:" + leaders);
        return leaderAndIsrRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<Tuple2<String, Integer>, Short> responseMap = Utils.map(partitionStateInfos, new Function2<Tuple2<String, Integer>, PartitionStateInfo, Tuple2<Tuple2<String, Integer>, Short>>() {
            @Override
            public Tuple2<Tuple2<String, Integer>, Short> apply(Tuple2<String, Integer> arg1, PartitionStateInfo arg2) {
                return Tuple2.make(arg1, ErrorMapping.codeFor(e.getClass()));
            }
        });
        
        LeaderAndIsrResponse errorResponse = new LeaderAndIsrResponse(correlationId, responseMap);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
