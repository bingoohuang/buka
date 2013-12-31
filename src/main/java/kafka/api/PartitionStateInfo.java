package kafka.api;

import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils.Callable1;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Set;

public class PartitionStateInfo {
    public LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch;
    public Set<Integer> allReplicas;

    public PartitionStateInfo(LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch, Set<Integer> allReplicas) {
        this.leaderIsrAndControllerEpoch = leaderIsrAndControllerEpoch;
        this.allReplicas = allReplicas;
    }

    public int replicationFactor() {
        return allReplicas.size();
    }

    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(leaderIsrAndControllerEpoch.controllerEpoch);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leader);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch);
        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.isr.size());
        Utils.foreach(leaderIsrAndControllerEpoch.leaderAndIsr.isr, new Callable1<Integer>() {
            @Override
            public void apply(Integer arg) {
                buffer.putInt(arg);
            }
        });

        buffer.putInt(leaderIsrAndControllerEpoch.leaderAndIsr.zkVersion);
        buffer.putInt(replicationFactor());

        Utils.foreach(allReplicas, new Callable1<Integer>() {
            @Override
            public void apply(Integer arg) {
                buffer.putInt(arg);
            }
        });
    }

    public int sizeInBytes() {
        return
                4 /* epoch of the controller that elected the leader */ +
                        4 /* leader broker id */ +
                        4 /* leader epoch */ +
                        4 /* number of replicas in isr */ +
                        4 * leaderIsrAndControllerEpoch.leaderAndIsr.isr.size() /* replicas in isr */ +
                        4 /* zk version */ +
                        4 /* replication factor */ +
                        allReplicas.size() * 4;
    }

    @Override
    public String toString() {
        StringBuilder partitionStateInfo = new StringBuilder();
        partitionStateInfo.append("(LeaderAndIsrInfo:" + leaderIsrAndControllerEpoch);
        partitionStateInfo.append(",ReplicationFactor:" + replicationFactor() + ")");
        partitionStateInfo.append(",AllReplicas:" + allReplicas + ")");
        return partitionStateInfo.toString();
    }
}
