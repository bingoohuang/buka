package kafka.api;

import kafka.network.InvalidRequestException;
import kafka.utils.Function1;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Set;

import static kafka.api.ApiUtils.readShortString;

public class StopReplicaRequestReader implements RequestReader{
    public static final RequestReader instance = new StopReplicaRequestReader();

    public static final short CurrentVersion = 0;
    public static final String DefaultClientId = "";
    public static final int DefaultAckTimeout = 100;

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        byte flag = buffer.get();
        if (flag != 0 && flag != 1)
            throw new InvalidRequestException("Invalid byte %d in delete partitions field. (Assuming false.)", flag);

        boolean deletePartitions = flag == 1;

        int topicPartitionPairCount = buffer.getInt();
        Set<Tuple2<String, Integer>> topicPartitionPairSet = Utils.flatSet(1, topicPartitionPairCount, new Function1<Integer, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> apply(Integer arg) {
                return Tuple2.make(readShortString(buffer), buffer.getInt());
            }
        });

        return new StopReplicaRequest(versionId, correlationId, clientId, controllerId, controllerEpoch,
                deletePartitions, topicPartitionPairSet);
    }


}
