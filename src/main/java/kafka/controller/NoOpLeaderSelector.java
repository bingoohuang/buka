package kafka.controller;

import com.google.common.collect.Lists;
import kafka.api.LeaderAndIsr;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
public class NoOpLeaderSelector implements PartitionLeaderSelector {
    public ControllerContext controllerContext;

    public NoOpLeaderSelector(ControllerContext controllerContext) {
        this.controllerContext = controllerContext;
    }

    Logger logger = LoggerFactory.getLogger(NoOpLeaderSelector.class);

    @Override
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr) {
        logger.warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.");
        List<Integer> t2 = Lists.newArrayList(controllerContext.partitionReplicaAssignment.get(topicAndPartition));
        return Tuple2.make(currentLeaderAndIsr, t2);
    }
}
