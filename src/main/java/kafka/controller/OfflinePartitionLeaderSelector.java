package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import kafka.api.LeaderAndIsr;
import kafka.common.NoReplicaOnlineException;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

/**
 * This API selects a new leader for the input partition -
 * 1. If at least one broker from the isr is alive, it picks a broker from the isr as the new leader
 * 2. Else, it picks some alive broker from the assigned replica list as the new leader
 * 3. If no broker in the assigned replica list is alive, it throws NoReplicaOnlineException
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
public class OfflinePartitionLeaderSelector implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public OfflinePartitionLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    Logger logger = LoggerFactory.getLogger(OfflinePartitionLeaderSelector.class);

    @Override
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        Collection<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        if (assignedReplicas == null)
            throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it", topicAndPartition);

        List<Integer> liveAssignedReplicasToThisPartition = Utils.filter(assignedReplicas, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return controllerContext.liveBrokerIds().contains(r);
            }
        });
        List<Integer> liveBrokersInIsr = Utils.filter(currentLeaderAndIsr.isr, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return controllerContext.liveBrokerIds().contains(r);
            }
        });

        int currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch;
        int currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion;
        LeaderAndIsr newLeaderAndIsr;
        if (liveBrokersInIsr.isEmpty()) {
            logger.debug("No broker in ISR is alive for {}. Pick the leader from the alive assigned replicas: {}",
                    topicAndPartition, liveAssignedReplicasToThisPartition);
            if (liveAssignedReplicasToThisPartition.isEmpty()) {
                throw new NoReplicaOnlineException("No replica for partition " +
                        "%s is alive. Live brokers are: [%s], Assigned replicas are: [%s]", topicAndPartition, controllerContext.liveBrokerIds()
                        , assignedReplicas);
            }

            ControllerStats.instance.uncleanLeaderElectionRate.mark();
            int newLeader = Utils.head(liveAssignedReplicasToThisPartition);
            logger.warn("No broker in ISR is alive for {}. Elect leader {} from live brokers {}. There's potential data loss.",
                    topicAndPartition, newLeader, liveAssignedReplicasToThisPartition);
            newLeaderAndIsr = new LeaderAndIsr(newLeader, currentLeaderEpoch + 1,
                    Lists.newArrayList(newLeader), currentLeaderIsrZkPathVersion + 1);
        } else {
            int newLeader = Utils.head(liveBrokersInIsr);
            logger.debug("Some broker in ISR is alive for {}. Select {} from ISR {} to be the leader.",
                    topicAndPartition, newLeader, liveBrokersInIsr);
            newLeaderAndIsr = new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr, currentLeaderIsrZkPathVersion + 1);
        }

        logger.info("Selected new leader and ISR {} for offline partition {}", newLeaderAndIsr.toString(), topicAndPartition);
        return Tuple2.make(newLeaderAndIsr, liveAssignedReplicasToThisPartition);
    }
}
