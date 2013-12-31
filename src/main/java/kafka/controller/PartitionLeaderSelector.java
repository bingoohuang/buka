package kafka.controller;

import kafka.api.LeaderAndIsr;
import kafka.common.TopicAndPartition;
import kafka.utils.Tuple2;

import java.util.List;

public interface PartitionLeaderSelector {
    /**
     * @param topicAndPartition          The topic and partition whose leader needs to be elected
     * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
     * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
     * @return The leader and isr request, with the newly selected leader info, to send to the brokers
     * Also, returns the list of replicas the returned leader and isr request should be sent to
     * This API selects a new leader for the input partition
     */
    public Tuple2<LeaderAndIsr, List<Integer>> selectLeader(TopicAndPartition topicAndPartition, LeaderAndIsr currentLeaderAndIsr);
}
