package kafka.controller;


import com.google.common.base.Predicate;
import kafka.api.LeaderAndIsr;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Picks one of the alive replicas (other than the current leader) in ISR as
 * new leader, fails if there are no other replicas in ISR.
 */
public class ControlledShutdownLeaderSelector implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public ControlledShutdownLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    Logger logger = LoggerFactory.getLogger(ControlledShutdownLeaderSelector.class);

    @Override
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;

        final int currentLeader = currentLeaderAndIsr.leader;

        Collection<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        final Set<Integer> liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds();
        List<Integer> liveAssignedReplicas = Utils.filter(assignedReplicas, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return liveOrShuttingDownBrokerIds.contains(r);
            }
        });

        List<Integer> newIsr = Utils.filter(currentLeaderAndIsr.isr, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer brokerId) {
                return brokerId != currentLeader &&
                        !controllerContext.shuttingDownBrokerIds.contains(brokerId);
            }
        });

        Integer newLeader = Utils.head(newIsr);
        if (newLeader != null) {
            logger.debug("Partition {} : current leader = {}, new leader = {}",
                    topicAndPartition, currentLeader, newLeader);
            return Tuple2.make(new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1),
                    liveAssignedReplicas);
        }

        throw new StateChangeFailedException("No other replicas in ISR %s for %s besides current leader %d and" +
                " shutting down brokers %s", currentLeaderAndIsr.isr, topicAndPartition, currentLeader, controllerContext.shuttingDownBrokerIds);
    }
}
