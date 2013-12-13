package kafka.api;

import kafka.cluster.Broker;
import kafka.cluster.Brokers;
import kafka.common.TopicAndPartition;
import kafka.utils.Function0;
import kafka.utils.Function1;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static kafka.api.ApiUtils.readShortString;

public class UpdateMetadataRequestReader implements RequestReader {
    public static final RequestReader instance = new UpdateMetadataRequestReader();

    public static final short CurrentVersion = 0;
    public static final boolean IsInit = true;
    public static final boolean NotInit = false;
    public static final int DefaultAckTimeout = 1000;

    @Override
    public RequestOrResponse readFrom(final ByteBuffer buffer) {
        short versionId = buffer.getShort();
        int correlationId = buffer.getInt();
        String clientId = readShortString(buffer);
        int controllerId = buffer.getInt();
        int controllerEpoch = buffer.getInt();
        int partitionStateInfosCount = buffer.getInt();


        Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos = Utils.flatMap(0, partitionStateInfosCount, new Function0<Tuple2<TopicAndPartition, PartitionStateInfo>>() {
            @Override
            public Tuple2<TopicAndPartition, PartitionStateInfo> apply() {
                String topic = readShortString(buffer);
                int partition = buffer.getInt();
                PartitionStateInfo partitionStateInfo = PartitionStateInfos.readFrom(buffer);

                return Tuple2.make(new TopicAndPartition(topic, partition), partitionStateInfo);
            }
        });


        int numAliveBrokers = buffer.getInt();
        Set<Broker> aliveBrokers = Utils.flatSet(0, numAliveBrokers, new Function1<Integer, Broker>() {
            @Override
            public Broker apply(Integer arg) {
                return Brokers.readFrom(buffer);
            }
        });

        return new UpdateMetadataRequest(versionId, correlationId, clientId, controllerId, controllerEpoch,
                partitionStateInfos, aliveBrokers);
    }
}
