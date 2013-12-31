package kafka.controller;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import kafka.api.LeaderAndIsr;
import kafka.api.RequestOrResponse;
import kafka.common.LeaderElectionNotNeededException;
import kafka.common.NoReplicaOnlineException;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 * deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 * replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 * Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 * moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
public class PartitionStateMachine {
    public KafkaController controller;

    public PartitionStateMachine(final KafkaController controller) {
        this.controller = controller;

        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;
        brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, new Callable3<Integer, RequestOrResponse, Callable1<RequestOrResponse>>() {
            @Override
            public void apply(Integer integer, RequestOrResponse requestOrResponse, Callable1<RequestOrResponse> requestOrResponseCallable1) {
                controller.sendRequest(integer,requestOrResponse,requestOrResponseCallable1);
            }
        },
                controllerId, controller.clientId());
        noOpPartitionLeaderSelector = new NoOpLeaderSelector(controllerContext);

        logger = LoggerFactory.getLogger(PartitionStateMachine.class + "[Partition state machine on Controller " + controllerId + "]: ");
    }

    private ControllerContext controllerContext;
    private int controllerId;
    private ZkClient zkClient;
    public Map<TopicAndPartition, PartitionState> partitionState = Maps.newHashMap();
    public ControllerBrokerRequestBatch brokerRequestBatch;
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    private NoOpLeaderSelector noOpPartitionLeaderSelector;
    Logger logger;
    private Logger stateChangeLogger = LoggerFactory.getLogger(KafkaControllers.stateChangeLogger);


    /**
     * Invoked on successful controller election. First registers a topic change listener since that triggers all
     * state transitions for partitions. Initializes the state of partitions by reading from zookeeper. Then triggers
     * the OnlinePartition state change for all new or offline partitions.
     */
    public void startup() {
        // initialize partition state
        initializePartitionState();
        hasStarted.set(true);
        // try to move partitions to online state
        triggerOnlinePartitionStateChange();
        logger.info("Started partition state machine with initial state -> " + partitionState.toString());
    }

    // register topic and partition change listeners
    public void registerListeners() {
        registerTopicChangeListener();
    }

    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        hasStarted.set(false);
        partitionState.clear();
    }


    /**
     * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
     * state. This is called on a successful controller election and on broker changes
     */
    public void triggerOnlinePartitionStateChange() {
        try {
            brokerRequestBatch.newBatch();
            // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state
            for (Map.Entry<TopicAndPartition, PartitionState> entry : partitionState.entrySet()) {
                TopicAndPartition topicAndPartition = entry.getKey();
                PartitionState partitionState = entry.getValue();
                if (partitionState.equals(PartitionState.OfflinePartition)
                        || partitionState.equals(PartitionState.NewPartition))
                    handleStateChange(topicAndPartition.topic, topicAndPartition.partition,
                            PartitionState.OnlinePartition, controller.offlinePartitionSelector);
            }
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e) {
            logger.error("Error while moving some partitions to the online state", e);
            // TODO: It is not enough to bail out and log an error, it is important to trigger leader election for those partitions
        }
    }


    /**
     * This API is invoked by the partition change zookeeper listener
     *
     * @param partitions  The list of partitions that need to be transitioned to the target state
     * @param targetState The state that the partitions should be moved to
     */
    public void handleStateChanges(Set<TopicAndPartition> partitions, final PartitionState targetState) {
        handleStateChanges(partitions, targetState, noOpPartitionLeaderSelector);
    }

    public void handleStateChanges(Set<TopicAndPartition> partitions, final PartitionState targetState,
                                   final PartitionLeaderSelector leaderSelector /*= noOpPartitionLeaderSelector*/) {
        logger.info("Invoking state change to {} for partitions {}", targetState, partitions);
        try {
            brokerRequestBatch.newBatch();
            Utils.foreach(partitions, new Callable1<TopicAndPartition>() {
                @Override
                public void apply(TopicAndPartition topicAndPartition) {
                    handleStateChange(topicAndPartition.topic, topicAndPartition.partition, targetState, leaderSelector);
                }
            });

            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e) {
            logger.error("Error while moving some partitions to {} state", targetState, e);
            // TODO: It is not enough to bail out and log an error, it is important to trigger state changes for those partitions
        }
    }


    /**
     * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state.
     *
     * @param topic       The topic of the partition for which the state transition is invoked
     * @param partition   The partition for which the state transition is invoked
     * @param targetState The end state that the partition should be moved to
     */
    private void handleStateChange(String topic, int partition, PartitionState targetState,
                                   PartitionLeaderSelector leaderSelector) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException("Controller %d epoch %d initiated state change for partition %s to %s failed because " +
                    "the partition state machine has not started",
                    controllerId, controller.epoch(), topicAndPartition, targetState);
        PartitionState currState = Utils.getOrElse(partitionState, topicAndPartition, PartitionState.NonExistentPartition);
        try {
            switch (targetState) {
                case NewPartition:
                    // pre: partition did not exist before this
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(PartitionState.NonExistentPartition), PartitionState.NewPartition);
                    assignReplicasToPartitions(topic, partition);
                    partitionState.put(topicAndPartition, PartitionState.NewPartition);

                    String assignedReplicas = Joiner.on(',').join(controllerContext.partitionReplicaAssignment.get(topicAndPartition));
                    stateChangeLogger.trace(String.format("Controller %d epoch %d changed partition %s state from NotExists to New with assigned replicas %s",
                            controllerId, controller.epoch(), topicAndPartition, assignedReplicas));
                    break;
                // post: partition has been assigned replicas
                case OnlinePartition:
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(PartitionState.NewPartition,
                            PartitionState.OnlinePartition, PartitionState.OfflinePartition), PartitionState.OnlinePartition);
                    switch (partitionState.get(topicAndPartition)) {
                        case NewPartition:
                            // initialize leader and isr path for new partition
                            initializeLeaderAndIsrForPartition(topicAndPartition);
                            break;
                        case OfflinePartition:
                            electLeaderForPartition(topic, partition, leaderSelector);
                            break;
                        case OnlinePartition: // invoked when the leader needs to be re-elected
                            electLeaderForPartition(topic, partition, leaderSelector);
                            break;
                        default: // should never come here since illegal previous states are checked above
                    }
                    partitionState.put(topicAndPartition, PartitionState.OnlinePartition);
                    int leader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
                    stateChangeLogger.trace("Controller {} epoch {} changed partition {} from {} to OnlinePartition with leader {}",
                            controllerId, controller.epoch(), topicAndPartition, partitionState.get(topicAndPartition), leader);
                    break;
                // post: partition has a leader
                case OfflinePartition:
                    // pre: partition should be in New or Online state
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(PartitionState.NewPartition, PartitionState.OnlinePartition), PartitionState.OfflinePartition);
                    // should be called when the leader for a partition is no longer alive
                    stateChangeLogger.trace("Controller {} epoch {} changed partition {} state from Online to Offline",
                            controllerId, controller.epoch(), topicAndPartition);
                    partitionState.put(topicAndPartition, PartitionState.OfflinePartition);
                    break;
                // post: partition has no alive leader
                case NonExistentPartition:
                    // pre: partition should be in Offline state
                    assertValidPreviousStates(topicAndPartition, Lists.newArrayList(PartitionState.OfflinePartition), PartitionState.NonExistentPartition);
                    stateChangeLogger.trace("Controller {} epoch {} changed partition {} state from Offline to NotExists",
                            controllerId, controller.epoch(), topicAndPartition);
                    partitionState.put(topicAndPartition, PartitionState.NonExistentPartition);
                    // post: partition state is deleted from all brokers and zookeeper
            }
        } catch (Throwable t) {
            stateChangeLogger.error(String.format("Controller %d epoch %d initiated state change for partition %s from %s to %s failed",
                    controllerId, controller.epoch(), topicAndPartition, currState, targetState), t);
        }
    }

    /**
     * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
     * zookeeper
     */
    private void initializePartitionState() {

        for (Map.Entry<TopicAndPartition, Integer> entry : controllerContext.partitionReplicaAssignment.entries()) {
            TopicAndPartition topicPartition = entry.getKey();
            Integer replicaAssignment = entry.getValue();

            // check if leader and isr path exists for partition. If not, then it is in NEW state
            LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = controllerContext.partitionLeadershipInfo.get(topicPartition);
            if (currentLeaderIsrAndEpoch != null) {
                // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
                if (controllerContext.liveBrokerIds().contains(currentLeaderIsrAndEpoch.leaderAndIsr.leader)) {
                    // leader is alive
                    partitionState.put(topicPartition, PartitionState.OnlinePartition);
                } else {
                    partitionState.put(topicPartition, PartitionState.OfflinePartition);
                }
            } else {
                partitionState.put(topicPartition, PartitionState.NewPartition);
            }
        }
    }

    private void assertValidPreviousStates(TopicAndPartition topicAndPartition, List<PartitionState> fromStates,
                                           PartitionState targetState) {
        if (!fromStates.contains(partitionState.get(topicAndPartition)))
            throw new IllegalStateException(String.format("Partition %s should be in the %s states before moving to %s state",
                    topicAndPartition, fromStates.toString(), targetState) + String.format(". Instead it is in %s state",
                    partitionState.get(topicAndPartition)));
    }


    /**
     * Invoked on the NonExistentPartition->NewPartition state transition to update the controller's cache with the
     * partition's replica assignment.
     *
     * @param topic     The topic of the partition whose replica assignment is to be cached
     * @param partition The partition whose replica assignment is to be cached
     */
    private void assignReplicasToPartitions(String topic, int partition) {
        List<Integer> assignedReplicas = ZkUtils.getReplicasForPartition(controllerContext.zkClient, topic, partition);
        controllerContext.partitionReplicaAssignment.putAll(new TopicAndPartition(topic, partition), assignedReplicas);
    }

    /**
     * Invoked on the NewPartition->OnlinePartition state change. When a partition is in the New state, it does not have
     * a leader and isr path in zookeeper. Once the partition moves to the OnlinePartition state, it's leader and isr
     * path gets initialized and it never goes back to the NewPartition state. From here, it can only go to the
     * OfflinePartition state.
     *
     * @param topicAndPartition The topic/partition whose leader and isr path is to be initialized
     */
    private void initializeLeaderAndIsrForPartition(TopicAndPartition topicAndPartition) {
        Collection<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        List<Integer> liveAssignedReplicas = Utils.filter(replicaAssignment, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return controllerContext.liveBrokerIds().contains(r);
            }
        });
        switch (liveAssignedReplicas.size()) {
            case 0:
                String failMsg = String.format("encountered error during state change of partition %s from New to Online, assigned replicas are [%s], " +
                        "live brokers are [%s]. No assigned replica is alive.",
                        topicAndPartition, replicaAssignment.toString(), controllerContext.liveBrokerIds());
                stateChangeLogger.error("Controller {} epoch {} ", controllerId, controller.epoch() + failMsg);
                throw new StateChangeFailedException(failMsg);
            default:
                logger.debug("Live assigned replicas for partition {} are: [{}]", topicAndPartition, liveAssignedReplicas);
                // make the first replica in the list of assigned replicas, the leader
                Integer leader = Utils.head(liveAssignedReplicas);
                LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, liveAssignedReplicas),
                        controller.epoch());
                logger.debug("Initializing leader and isr for partition {} to {}", topicAndPartition, leaderIsrAndControllerEpoch);
                try {
                    ZkUtils.createPersistentPath(controllerContext.zkClient,
                            ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                            ZkUtils.leaderAndIsrZkData(leaderIsrAndControllerEpoch.leaderAndIsr, controller.epoch()));
                    // NOTE: the above write can fail only if the current controller lost its zk session and the new controller
                    // took over and initialized this partition. This can happen if the current controller went into a long
                    // GC pause
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, leaderIsrAndControllerEpoch);
                    brokerRequestBatch.addLeaderAndIsrRequestForBrokers(liveAssignedReplicas, topicAndPartition.topic,
                            topicAndPartition.partition, leaderIsrAndControllerEpoch, Sets.newHashSet(replicaAssignment));
                } catch (ZkNodeExistsException e) {
                    // read the controller epoch
                    LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topicAndPartition.topic,
                            topicAndPartition.partition);
                    failMsg = String.format("encountered error while changing partition %s's state from New to Online since LeaderAndIsr path already " +
                            "exists with value %s and controller epoch %d",
                            topicAndPartition, leaderIsrAndEpoch.leaderAndIsr.toString(), leaderIsrAndEpoch.controllerEpoch);
                    stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
                    throw new StateChangeFailedException(failMsg);
                }
        }
    }

    /**
     * Invoked on the OfflinePartition->OnlinePartition state change. It invokes the leader election API to elect a leader
     * for the input offline partition
     *
     * @param topic          The topic of the offline partition
     * @param partition      The offline partition
     * @param leaderSelector Specific leader selector (e.g., offline/reassigned/etc.)
     */
    public void electLeaderForPartition(String topic, int partition, PartitionLeaderSelector leaderSelector) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        // handle leader election for the partitions whose leader is no longer alive
        stateChangeLogger.trace(String.format("Controller %d epoch %d started leader election for partition %s",
                controllerId, controller.epoch(), topicAndPartition));
        try {
            Boolean zookeeperPathUpdateSucceeded = false;
            LeaderAndIsr newLeaderAndIsr = null;
            List<Integer> replicasForThisPartition = Lists.newArrayList();
            while (!zookeeperPathUpdateSucceeded) {
                LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = getLeaderIsrAndEpochOrThrowException(topic, partition);
                LeaderAndIsr currentLeaderAndIsr = currentLeaderIsrAndEpoch.leaderAndIsr;
                int controllerEpoch = currentLeaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > controller.epoch()) {
                    String failMsg = String.format("aborted leader election for partition [%s,%d] since the LeaderAndIsr path was " +
                            "already written by another controller. This probably means that the current controller %d went through " +
                            "a soft failure and another controller was elected with epoch %d.",
                            topic, partition, controllerId, controllerEpoch);
                    stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
                    throw new StateChangeFailedException(failMsg);
                }
                // elect new leader or throw exception
                Tuple2<LeaderAndIsr, List<Integer>> tuple2 = leaderSelector.selectLeader(topicAndPartition, currentLeaderAndIsr);
                LeaderAndIsr leaderAndIsr = tuple2._1;
                List<Integer> replicas = tuple2._2;


                Tuple2<Boolean, Integer> ret = ZkUtils.conditionalUpdatePersistentPath(zkClient,
                        ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                        ZkUtils.leaderAndIsrZkData(leaderAndIsr, controller.epoch()), currentLeaderAndIsr.zkVersion);
                Boolean updateSucceeded = ret._1;
                Integer newVersion = ret._2;
                newLeaderAndIsr = leaderAndIsr;
                newLeaderAndIsr.zkVersion = newVersion;
                zookeeperPathUpdateSucceeded = updateSucceeded;
                replicasForThisPartition = replicas;
            }
            LeaderIsrAndControllerEpoch newLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, controller.epoch());
            // update the leader cache
            controllerContext.partitionLeadershipInfo.put(new TopicAndPartition(topic, partition), newLeaderIsrAndControllerEpoch);
            stateChangeLogger.trace(String.format("Controller %d epoch %d elected leader %d for Offline partition %s",
                    controllerId, controller.epoch(), newLeaderAndIsr.leader, topicAndPartition));
            Collection<Integer> replicas = controllerContext.partitionReplicaAssignment.get(new TopicAndPartition(topic, partition));
            // store new leader and isr info in cache
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasForThisPartition, topic, partition,
                    newLeaderIsrAndControllerEpoch, Sets.newHashSet(replicas));
        } catch (LeaderElectionNotNeededException e) {
            // swallow
        } catch (NoReplicaOnlineException e) {
            throw e;
        } catch (Throwable e) {
            String failMsg = String.format("encountered error while electing leader for partition %s due to: %s.", topicAndPartition, e.getMessage());
            stateChangeLogger.error(String.format("Controller %d epoch %d ", controllerId, controller.epoch()) + failMsg);
            throw new StateChangeFailedException(failMsg, e);
        }
        logger.debug("After leader election, leader cache is updated to {}", controllerContext.partitionLeadershipInfo.toString());
    }

    private void registerTopicChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, new TopicChangeListener());
    }


    public void registerPartitionChangeListener(String topic) {
        zkClient.subscribeDataChanges(ZkUtils.getTopicPath(topic), new AddPartitionsListener(topic));
    }

    private LeaderIsrAndControllerEpoch getLeaderIsrAndEpochOrThrowException(String topic, int partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        LeaderIsrAndControllerEpoch currentLeaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
        if (currentLeaderIsrAndEpoch != null) return currentLeaderIsrAndEpoch;

        String failMsg = String.format("LeaderAndIsr information doesn't exist for partition %s in %s state",
                topicAndPartition, partitionState.get(topicAndPartition));
        throw new StateChangeFailedException(failMsg);
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a partition
     */
    class TopicChangeListener implements IZkChildListener {
        Logger logger = LoggerFactory.getLogger(TopicChangeListener.class + "[TopicChangeListener on Controller " + controller.config.brokerId + "]: ");


        @Override
        public void handleChildChange(String parentPath, List<String> children) throws Exception {
            synchronized (controllerContext.controllerLock) {
                if (hasStarted.get()) {
                    try {
                        logger.debug("Topic change listener fired for path {} with children {}", parentPath, children);
                        Set<String> currentChildren = Sets.newHashSet(children);
                        Set<String> newTopics = Sets.newHashSet(currentChildren);
                        newTopics.removeAll(controllerContext.allTopics);


                        final Set<String> deletedTopics = Sets.newHashSet(controllerContext.allTopics);
                        deletedTopics.removeAll(currentChildren);

                        //        val deletedPartitionReplicaAssignment = replicaAssignment.filter(p => deletedTopics.contains(p._1._1))
                        controllerContext.allTopics = currentChildren;

                        Multimap<TopicAndPartition, Integer> addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(newTopics));
                        controllerContext.partitionReplicaAssignment = Utils.filter(controllerContext.partitionReplicaAssignment, new Predicate<Map.Entry<TopicAndPartition, Integer>>() {
                            @Override
                            public boolean apply(Map.Entry<TopicAndPartition, Integer> p) {
                                return !deletedTopics.contains(p.getKey().topic);
                            }
                        });
                        controllerContext.partitionReplicaAssignment.putAll(addedPartitionReplicaAssignment);
                        logger.info("New topics: [{}], deleted topics: [{}], new partition replica assignment [{}]", newTopics,
                                deletedTopics, addedPartitionReplicaAssignment);
                        if (newTopics.size() > 0)
                            controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet());
                    } catch (Throwable e) {
                        logger.error("Error while handling new topic", e);
                    }
                    // TODO: kafka-330  Handle deleted topics
                }
            }
        }
    }


    class AddPartitionsListener implements IZkDataListener {
        public String topic;

        AddPartitionsListener(String topic) {
            this.topic = topic;
        }

        Logger logger = LoggerFactory.getLogger(AddPartitionsListener.class + "[AddPartitionsListener on " + controller.config.brokerId + "]: ");


        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            synchronized (controllerContext.controllerLock) {
                try {
                    logger.info("Add Partition triggered {} for path {}", data.toString(), dataPath);
                    Multimap<TopicAndPartition, Integer> partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(topic));
                    Multimap<TopicAndPartition, Integer> partitionsRemainingToBeAdded = Utils.filter(partitionReplicaAssignment, new Predicate<Map.Entry<TopicAndPartition, Integer>>() {
                        @Override
                        public boolean apply(Map.Entry<TopicAndPartition, Integer> p) {
                            return !controllerContext.partitionReplicaAssignment.containsKey(p.getKey());
                        }
                    });

                    logger.info("New partitions to be added [{}]", partitionsRemainingToBeAdded.toString());
                    if (partitionsRemainingToBeAdded.size() > 0)
                        controller.onNewPartitionCreation(partitionsRemainingToBeAdded.keySet());
                } catch (Throwable e) {
                    logger.error("Error while handling add partitions for data path " + dataPath, e);
                }
            }
        }

        @Override
        public void handleDataDeleted(String parentPath) throws Exception {
            // this is not implemented for partition change
        }
    }
}
