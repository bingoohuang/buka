package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.utils.Function1;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ControllerContext {
    public ZkClient zkClient;
    public int zkSessionTimeout;
    public ControllerChannelManager controllerChannelManager;
    public Object controllerLock;
    public Set<Integer> shuttingDownBrokerIds;
    public Object brokerShutdownLock;
    public int epoch;
    public int epochZkVersion;
    public AtomicInteger correlationId;
    public Set<String> allTopics;
    public Multimap<TopicAndPartition, Integer> partitionReplicaAssignment;
    public Map<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionLeadershipInfo;
    public Map<TopicAndPartition, ReassignedPartitionsContext>  partitionsBeingReassigned;
    public Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection;

    public ControllerContext(ZkClient zkClient,
                             int zkSessionTimeout) {
        this(zkClient, zkSessionTimeout, null, new Object(),
                Sets.<Integer>newHashSet(), new Object(),
                KafkaControllers.InitialControllerEpoch - 1,
                KafkaControllers.InitialControllerEpochZkVersion - 1,
                new AtomicInteger(0),
                Sets.<String>newHashSet(),
                HashMultimap.<TopicAndPartition, Integer>create(),
                Maps.<TopicAndPartition, LeaderIsrAndControllerEpoch>newHashMap(),
                Maps.<TopicAndPartition, ReassignedPartitionsContext>newHashMap(),
                Sets.<TopicAndPartition>newHashSet());
    }

    public ControllerContext(ZkClient zkClient,
                             int zkSessionTimeout,
                             ControllerChannelManager controllerChannelManager,
                             Object controllerLock,
                             Set<Integer> shuttingDownBrokerIds,
                             Object brokerShutdownLock,
                             int epoch,
                             int epochZkVersion,
                             AtomicInteger correlationId,
                             Set<String> allTopics,
                             Multimap<TopicAndPartition, Integer> partitionReplicaAssignment,
                             Map<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionLeadershipInfo,
                             Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned,
                             Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection) {
        this.zkClient = zkClient;
        this.zkSessionTimeout = zkSessionTimeout;
        this.controllerChannelManager = controllerChannelManager;
        this.controllerLock = controllerLock;
        this.shuttingDownBrokerIds = shuttingDownBrokerIds;
        this.brokerShutdownLock = brokerShutdownLock;
        this.epoch = epoch;
        this.epochZkVersion = epochZkVersion;
        this.correlationId = correlationId;
        this.allTopics = allTopics;
        this.partitionReplicaAssignment = partitionReplicaAssignment;
        this.partitionLeadershipInfo = partitionLeadershipInfo;
        this.partitionsBeingReassigned = partitionsBeingReassigned;
        this.partitionsUndergoingPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection;
    }

    private Set<Broker> liveBrokersUnderlying = Sets.newHashSet();
    private Set<Integer> liveBrokerIdsUnderlying = Sets.newHashSet();

    // setter
    public void liveBrokers(Set<Broker> brokers) {
        liveBrokersUnderlying = brokers;
        liveBrokerIdsUnderlying = Utils.mapSet(liveBrokersUnderlying, new Function1<Broker, Integer>() {
            @Override
            public Integer apply(Broker arg) {
                return arg.id;
            }
        });
    }

    // getter
    public Set<Broker> liveBrokers() {
        return Utils.filterSet(liveBrokersUnderlying, new Predicate<Broker>(){
            @Override
            public boolean apply(Broker broker) {
                return !shuttingDownBrokerIds.contains(broker.id);
            }
        });
    }

    public Set<Integer> liveBrokerIds() {
        return Utils.filterSet(liveBrokerIdsUnderlying, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer brokerId) {
                return !shuttingDownBrokerIds.contains(brokerId);
            }
        });
    }

    public Set<Integer> liveOrShuttingDownBrokerIds() {
        return liveBrokerIdsUnderlying;
    }

    public Set<Broker> liveOrShuttingDownBrokers() {
        return liveBrokersUnderlying;
    }
}
