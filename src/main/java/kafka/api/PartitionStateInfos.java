package kafka.api;

import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.utils.Function1;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public class PartitionStateInfos {
    public static PartitionStateInfo readFrom(final ByteBuffer buffer) {
        int controllerEpoch = buffer.getInt();
        int leader = buffer.getInt();
        int leaderEpoch = buffer.getInt();
        int isrSize = buffer.getInt();
        List<Integer> isr = Utils.flatList(0, isrSize, new Function1<Integer, Integer>() {
            @Override
            public Integer apply(Integer index) {
                return buffer.getInt();
            }
        });


        int zkVersion = buffer.getInt();
        int replicationFactor = buffer.getInt();
        Set<Integer> replicas = Utils.flatSet(0, replicationFactor, new Function1<Integer, Integer>() {
            @Override
            public Integer apply(Integer index) {
                return buffer.getInt();
            }
        });


        return new PartitionStateInfo(
                new LeaderIsrAndControllerEpoch(
                        new LeaderAndIsr(leader, leaderEpoch, isr, zkVersion), controllerEpoch),
                replicas);
    }
}
