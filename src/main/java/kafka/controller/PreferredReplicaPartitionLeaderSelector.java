package kafka.controller;

import com.google.common.collect.Lists;
import kafka.api.LeaderAndIsr;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Picks the preferred replica as the new leader if -
 * 1. It is already not the current leader
 * 2. It is alive
 */
public class PreferredReplicaPartitionLeaderSelector implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public PreferredReplicaPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    Logger logger = LoggerFactory.getLogger(PreferredReplicaPartitionLeaderSelector.class);

    @Override
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        List<Integer> assignedReplicas = Lists.newArrayList(controllerContext.partitionReplicaAssignment.get(topicAndPartition));
        Integer preferredReplica = Utils.head(assignedReplicas);
        // check if preferred replica is the current leader
        int currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        if (currentLeader == preferredReplica) {
            throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s",
                    preferredReplica, topicAndPartition);
        }

        logger.info("Current leader {} for partition {} is not the preferred replica. Trigerring preferred replica leader election", currentLeader, topicAndPartition);

        // check if preferred replica is not the current leader and is alive and in the isr
        if (controllerContext.liveBrokerIds().contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
            return Tuple2.make(new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
                    currentLeaderAndIsr.zkVersion + 1), assignedReplicas);
        }
        throw new StateChangeFailedException("Preferred replica %d for partition %s is either not alive or not in the isr. Current leader and ISR: [%s]",
                preferredReplica, topicAndPartition, currentLeaderAndIsr);
    }
}
