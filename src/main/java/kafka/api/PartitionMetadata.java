package kafka.api;

import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.utils.Callable1;
import kafka.utils.Function1;
import kafka.utils.Range;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.readIntInRange;
import static kafka.api.ApiUtils.readShortInRange;

public class PartitionMetadata {
    public static PartitionMetadata readFrom(final ByteBuffer buffer, final Map<Integer, Broker> brokers) {
        short errorCode = readShortInRange(buffer, "error code", Range.make((short) -1, Short.MAX_VALUE));
        int partitionId = readIntInRange(buffer, "partition id", Range.make(0, Integer.MAX_VALUE)); /* partition id */
        int leaderId = buffer.getInt();
        Broker leader = brokers.get(leaderId);

    /* list of all replicas */
        int numReplicas = readIntInRange(buffer, "number of all replicas", Range.make(0, Integer.MAX_VALUE));

        List<Integer> replicaIds = Utils.flatList(0, numReplicas, new Function1<Integer, Integer>() {
            @Override
            public Integer apply(Integer arg) {
                return buffer.getInt();
            }
        });
        List<Broker> replicas = Utils.mapList(replicaIds, brokers);

    /* list of in-sync replicas */
        int numIsr = readIntInRange(buffer, "number of in-sync replicas", Range.make(0, Integer.MAX_VALUE));
        List<Integer> isrIds = Utils.flatList(0, numIsr, new Function1<Integer, Integer>() {
            @Override
            public Integer apply(Integer arg) {
                return buffer.getInt();
            }
        });

        List<Broker> isr = Utils.mapList(isrIds, brokers);

        return new PartitionMetadata(partitionId, leader, replicas, isr, errorCode);
    }

    public int partitionId;
    public Broker leader;
    public List<Broker> replicas;
    public List<Broker> isr;
    public short errorCode;

    public PartitionMetadata(int partitionId, Broker leader, List<Broker> replicas) {
        this(partitionId, leader, replicas, Lists.<Broker>newArrayList(), ErrorMapping.NoError);
    }

    public PartitionMetadata(int partitionId, Broker leader, List<Broker> replicas, List<Broker> isr, short errorCode) {
        this.partitionId = partitionId;
        this.leader = leader;
        this.replicas = replicas;
        this.isr = isr;
        this.errorCode = errorCode;
    }

    public int sizeInBytes() {
        return 2 /* error code */ +
                4 /* partition id */ +
                4 /* leader */ +
                4 + 4 * replicas.size() /* replica array */ +
                4 + 4 * isr.size(); /* isr array */
    }

    public void writeTo(final ByteBuffer buffer) {
        buffer.putShort(errorCode);
        buffer.putInt(partitionId);

    /* leader */
        int leaderId = leader != null ? leader.id : TopicMetadata.NoLeaderNodeId;
        buffer.putInt(leaderId);

    /* number of replicas */
        buffer.putInt(replicas.size());
        Utils.foreach(replicas, new Callable1<Broker>() {
            @Override
            public void apply(Broker broker) {
                buffer.putInt(broker.id);
            }
        });

    /* number of in-sync replicas */
        buffer.putInt(isr.size());
        Utils.foreach(isr, new Callable1<Broker>() {
            @Override
            public void apply(Broker broker) {
                buffer.putInt(broker.id);
            }
        });
    }

    @Override
    public String toString() {
        StringBuilder partitionMetadataString = new StringBuilder();
        partitionMetadataString.append("\tpartition " + partitionId);
        partitionMetadataString.append("\tleader: " + (leader != null ? leader : "none"));
        partitionMetadataString.append("\treplicas: " + replicas);
        partitionMetadataString.append("\tisr: " + isr);
        partitionMetadataString.append(String.format("\tisUnderReplicated: %s", (isr.size() < replicas.size() ? "true" : "false")));
        return partitionMetadataString.toString();
    }

}
