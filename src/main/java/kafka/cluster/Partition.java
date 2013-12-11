package kafka.cluster;

import kafka.server.ReplicaManager;
import kafka.utils.Time;

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
public class Partition {
    public final String topic;
    public final int partitionId;
    public final int replicationFactor;
    public final Time time;
    public final ReplicaManager replicaManager;

    public Partition(String topic, int partitionId, int replicationFactor, Time time, ReplicaManager replicaManager) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
        this.time = time;
        this.replicaManager = replicaManager;
    }
}
