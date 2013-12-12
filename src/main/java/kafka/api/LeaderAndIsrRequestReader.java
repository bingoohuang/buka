package kafka.api;

import com.google.common.collect.ImmutableMap;
import kafka.cluster.Broker;
import kafka.cluster.Brokers;
import kafka.utils.Function0;
import kafka.utils.Function1;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static kafka.api.ApiUtils.readShortString;

public class LeaderAndIsrRequestReader implements RequestReader {
    public static final RequestReader instance = new LeaderAndIsrRequestReader();

    public final static short CurrentVersion = 0;
    public final static boolean IsInit = true;
    public final static boolean NotInit = false;
    public final static int DefaultAckTimeout = 1000;

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();
        Map<Tuple2<String, Integer>, PartitionStateInfo> partitionStateInfos =
                Utils.flatMap(0, partitionStateInfosCount, new Function0<Map<Tuple2<String, Integer>, PartitionStateInfo>>() {
                    @Override
                    public Map<Tuple2<String, Integer>, PartitionStateInfo> apply() {
                        String topic = readShortString(buffer);
                        int partition = buffer.getInt();
                        PartitionStateInfo partitionStateInfo = PartitionStateInfos.readFrom(buffer);

                        return ImmutableMap.of(Tuple2.make(topic, partition), partitionStateInfo);
                    }
                });

        int leadersCount = buffer.getInt();
        Set<Broker> leaders = Utils.flatSet(0, leadersCount, new Function1<Integer, Broker>() {
            @Override
            public Broker apply(Integer arg) {
                return Brokers.readFrom(buffer);
            }
        });

        return new LeaderAndIsrRequest(versionId, correlationId, clientId, controllerId, controllerEpoch, partitionStateInfos, leaders);
    }
}
