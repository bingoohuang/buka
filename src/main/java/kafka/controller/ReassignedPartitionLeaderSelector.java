package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import kafka.api.LeaderAndIsr;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * Picks one of the alive in-sync reassigned replicas as the new leader.
 */
public class ReassignedPartitionLeaderSelector implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public ReassignedPartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    // Logger logger = LoggerFactory.getLogger(ReassignedPartitionLeaderSelector.class);

    /**
     * The reassigned replicas are already in the ISR when selectLeader is called.
     */
    @Override
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        Collection<Integer> reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned.get(topicAndPartition).newReplicas;
        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
        List<Integer> aliveReassignedInSyncReplicas = Utils.filter(reassignedInSyncReplicas, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return controllerContext.liveBrokerIds().contains(r);
            }
        });
        Integer newLeader = Utils.head(aliveReassignedInSyncReplicas);
        if (newLeader != null) {
            List<Integer> t2 = Lists.newArrayList(reassignedInSyncReplicas);
            return Tuple2.make(new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
                    currentLeaderIsrZkPathVersion + 1), t2);
        }

        switch (reassignedInSyncReplicas.size()) {
            case 0:
                throw new StateChangeFailedException("List of reassigned replicas for partition " +
                        " %s is empty. Current leader and ISR: [%s]", topicAndPartition, currentLeaderAndIsr);
            default:
                throw new StateChangeFailedException("None of the reassigned replicas for partition " +
                        "%s are alive. Current leader and ISR: [%s]", topicAndPartition, currentLeaderAndIsr);
        }
    }
}
