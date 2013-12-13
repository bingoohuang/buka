package kafka.api;

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

public class StopReplicaRequest extends RequestOrResponse {
    public short versionId;
    public String clientId;
    public int controllerId;
    public int controllerEpoch;
    public boolean deletePartitions;
    public Set<Tuple2<String, Integer>> partitions;

    public StopReplicaRequest(short versionId,
                              int correlationId,
                              String clientId,
                              int controllerId,
                              int controllerEpoch,
                              boolean deletePartitions,
                              Set<Tuple2<String, Integer>> partitions) {
        super(RequestKeys.StopReplicaKey, correlationId);
        this.versionId = versionId;
        this.clientId = clientId;
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.deletePartitions = deletePartitions;
        this.partitions = partitions;
    }

    public StopReplicaRequest(boolean deletePartitions,
                              Set<Tuple2<String, Integer>> partitions,
                              int controllerId,
                              int controllerEpoch,
                              int correlationId) {
        this(StopReplicaRequestReader.CurrentVersion, correlationId, StopReplicaRequestReader.DefaultClientId,
                controllerId, controllerEpoch, deletePartitions, partitions);
    }

    @Override
    public int sizeInBytes() {
        return 2 + /* versionId */
                4 + /* correlation id */
                ApiUtils.shortStringLength(clientId) +
                4 + /* controller id*/
                4 + /* controller epoch */
                1 + /* deletePartitions */
                4 /* partition count */
                + Utils.foldLeft(partitions, 0, new Function2<Integer, Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer apply(Integer arg1, Tuple2<String, Integer> arg2) {
                return arg1 + (ApiUtils.shortStringLength(arg2._1)) +
                        4 /* partition id */;
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

        buffer.put((byte) (deletePartitions ? 1 : 0));
        buffer.putInt(partitions.size());

        Utils.foreach(partitions, new Callable1<Tuple2<String,Integer>>() {
            @Override
            public void apply(Tuple2<String, Integer> arg) {
                String topic = arg._1;
                int partitionId = arg._2;

                writeShortString(buffer, topic);
                buffer.putInt(partitionId);
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder stopReplicaRequest = new StringBuilder();
        stopReplicaRequest.append("Name: " + this.getClass().getSimpleName());
        stopReplicaRequest.append("; Version: " + versionId);
        stopReplicaRequest.append("; CorrelationId: " + correlationId);
        stopReplicaRequest.append("; ClientId: " + clientId);
        stopReplicaRequest.append("; DeletePartitions: " + deletePartitions);
        stopReplicaRequest.append("; ControllerId: " + controllerId);
        stopReplicaRequest.append("; ControllerEpoch: " + controllerEpoch);
        stopReplicaRequest.append("; Partitions: " + partitions);
        return stopReplicaRequest.toString();
    }

    @Override
    public void handleError(final Throwable e, RequestChannel requestChannel, Request request) {
        Map<Tuple2<String, Integer>, Short> responseMap = Utils.map(partitions, new Function1<Tuple2<String, Integer>, Tuple2<Tuple2<String, Integer>, Short>>() {
            @Override
            public Tuple2<Tuple2<String, Integer>, Short> apply(Tuple2<String, Integer> topicAndPartition) {
                return Tuple2.make(topicAndPartition, ErrorMapping.codeFor(e.getClass()));
            }
        });

        StopReplicaResponse errorResponse = new StopReplicaResponse(correlationId, responseMap);
        requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(errorResponse)));
    }
}
