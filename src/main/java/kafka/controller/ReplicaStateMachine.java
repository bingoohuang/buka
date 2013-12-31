package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.common.StateChangeFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 * replica can only get become follower state change request.  Valid previous
 * state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 * state. In this state, it can get either become leader or become follower state change requests.
 * Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 * is down. Valid previous state are NewReplica, OnlineReplica
 * 4. NonExistentReplica: If a replica is deleted, it is moved to this state. Valid previous state is OfflineReplica
 */
public class ReplicaStateMachine {
    public KafkaController controller;

    public ReplicaStateMachine(final KafkaController controller) {
        this.controller = controller;

        controllerContext = controller.controllerContext;
        controllerId = controller.config.brokerId;
        zkClient = controllerContext.zkClient;
        brokerRequestBatch = new ControllerBrokerRequestBatch(controller.controllerContext, new Callable3<Integer, RequestOrResponse, Callable1<RequestOrResponse>>() {
            @Override
            public void apply(Integer integer, RequestOrResponse requestOrResponse, Callable1<RequestOrResponse> requestOrResponseCallable1) {
                controller.sendRequest(integer, requestOrResponse, requestOrResponseCallable1);
            }
        },
                controllerId, controller.clientId());

        logger = LoggerFactory.getLogger(ReplicaStateMachine.class + "[Replica state machine on controller " + controller.config.brokerId + "]: ");
    }

    private ControllerContext controllerContext;
    private int controllerId;
    private ZkClient zkClient;
    public Map<Tuple3<String, Integer, Integer>, ReplicaState> replicaState = Maps.newHashMap();
    ControllerBrokerRequestBatch brokerRequestBatch;
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    Logger logger;
    private Logger stateChangeLogger = LoggerFactory.getLogger(KafkaControllers.stateChangeLogger);

    /**
     * Invoked on successful controller election. First registers a broker change listener since that triggers all
     * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
     * Then triggers the OnlineReplica state change for all replicas.
     */
    public void startup() {
        // initialize replica state
        initializeReplicaState();
        hasStarted.set(true);
        // move all Online replicas to Online
        handleStateChanges(getAllReplicasOnBroker(controllerContext.allTopics,
                controllerContext.liveBrokerIds()), ReplicaState.OnlineReplica);
        logger.info("Started replica state machine with initial state -> {}", replicaState.toString());
    }

    // register broker change listener
    public void registerListeners() {
        registerBrokerChangeListener();
    }


    /**
     * Invoked on controller shutdown.
     */
    public void shutdown() {
        hasStarted.set(false);
        replicaState.clear();
    }

    /**
     * This API is invoked by the broker change controller callbacks and the startup API of the state machine
     *
     * @param replicas    The list of replicas (brokers) that need to be transitioned to the target state
     * @param targetState The state that the replicas should be moved to
     *                    The controller's allLeaders cache should have been updated before this
     */
    public void handleStateChanges(Set<PartitionAndReplica> replicas, final ReplicaState targetState) {
        logger.info("Invoking state change to {} for replicas {}", targetState, replicas);
        try {
            brokerRequestBatch.newBatch();
            Utils.foreach(replicas, new Callable1<PartitionAndReplica>() {
                @Override
                public void apply(PartitionAndReplica r) {
                    handleStateChange(r.topic, r.partition, r.replica, targetState);
                }
            });
            brokerRequestBatch.sendRequestsToBrokers(controller.epoch(), controllerContext.correlationId.getAndIncrement());
        } catch (Throwable e) {
            logger.error("Error while moving some replicas to {} state", targetState, e);
        }
    }

    /**
     * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
     * previous state to the target state.
     *
     * @param topic       The topic of the replica for which the state transition is invoked
     * @param partition   The partition of the replica for which the state transition is invoked
     * @param replicaId   The replica for which the state transition is invoked
     * @param targetState The end state that the replica should be moved to
     */
    public void handleStateChange(String topic, int partition, final int replicaId, ReplicaState targetState) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        if (!hasStarted.get())
            throw new StateChangeFailedException("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                    "to %s failed because replica state machine has not started",
                    controllerId, controller.epoch(), replicaId, topicAndPartition, targetState);
        try {
            Utils.getOrElseUpdate(replicaState, Tuple3.make(topic, partition, replicaId), ReplicaState.NonExistentReplica);
            Collection<Integer> replicaAssignment = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            switch (targetState) {
                case NewReplica:
                    assertValidPreviousStates(topic, partition, replicaId, Lists.newArrayList(ReplicaState.NonExistentReplica), targetState);
                    // start replica as a follower to the current leader for its partition
                    LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
                    if (leaderIsrAndControllerEpoch != null) {
                        if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                            throw new StateChangeFailedException(String.format("Replica %d for partition %s cannot be moved to NewReplica",
                                    replicaId, topicAndPartition) + "state as it is being requested to become leader");
                        brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Lists.newArrayList(replicaId),
                                topic, partition, leaderIsrAndControllerEpoch,
                                Sets.newHashSet(replicaAssignment));
                    } else {
                        // new leader request will be sent to this replica when one gets elected
                    }

                    replicaState.put(Tuple3.make(topic, partition, replicaId), ReplicaState.NewReplica);
                    stateChangeLogger.trace("Controller {} epoch {} changed state of replica {} for partition {} to NewReplica",
                            controllerId, controller.epoch(), replicaId, topicAndPartition);
                    break;
                case NonExistentReplica:
                    assertValidPreviousStates(topic, partition, replicaId, Lists.newArrayList(ReplicaState.OfflineReplica), targetState);
                    // send stop replica command
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, /*deletePartition = */true);
                    // remove this replica from the assigned replicas list for its partition
                    Collection<Integer> currentAssignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                    controllerContext.partitionReplicaAssignment.putAll(topicAndPartition, Utils.filter(currentAssignedReplicas, new Predicate<Integer>() {
                        @Override
                        public boolean apply(Integer _) {
                            return _ != replicaId;
                        }
                    }));
                    replicaState.remove(Tuple3.make(topic, partition, replicaId));
                    stateChangeLogger.trace("Controller {} epoch {} changed state of replica {} for partition {} to NonExistentReplica",
                            controllerId, controller.epoch(), replicaId, topicAndPartition);
                    break;
                case OnlineReplica:
                    assertValidPreviousStates(topic, partition, replicaId, Lists.newArrayList(ReplicaState.NewReplica, ReplicaState.OnlineReplica, ReplicaState.OfflineReplica), targetState);
                    ReplicaState replicaState1 = replicaState.get(Tuple3.make(topic, partition, replicaId));
                    if (replicaState1.equals(ReplicaState.NewReplica)) {
                        // add this replica to the assigned replicas list for its partition
                        Collection<Integer> currentAssignedReplicas1 = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
                        if (!currentAssignedReplicas1.contains(replicaId))
                            controllerContext.partitionReplicaAssignment.put(topicAndPartition, replicaId);
                        stateChangeLogger.trace("Controller {} epoch {} changed state of replica {} for partition {} to OnlineReplica",
                                controllerId, controller.epoch(), replicaId, topicAndPartition);
                    } else {
                        // check if the leader for this partition ever existed
                        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch1 = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                        if (leaderIsrAndControllerEpoch1 != null) {
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, leaderIsrAndControllerEpoch1,
                                    Sets.newHashSet(replicaAssignment));
                            replicaState.put(Tuple3.make(topic, partition, replicaId), ReplicaState.OnlineReplica);
                            stateChangeLogger.trace("Controller {} epoch {} changed state of replica {} for partition {} to OnlineReplica",
                                    controllerId, controller.epoch(), replicaId, topicAndPartition);
                        } else { // that means the partition was never in OnlinePartition state, this means the broker never
                            // started a log for that partition and does not have a high watermark value for this partition
                        }
                    }


                    replicaState.put(Tuple3.make(topic, partition, replicaId), ReplicaState.OnlineReplica);
                    break;
                case OfflineReplica:
                    assertValidPreviousStates(topic, partition, replicaId, Lists.newArrayList(ReplicaState.NewReplica, ReplicaState.OnlineReplica), targetState);
                    // send stop replica command to the replica so that it stops fetching from the leader
                    brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(replicaId), topic, partition, /*deletePartition = */false);
                    // As an optimization, the controller removes dead replicas from the ISR
                    boolean leaderAndIsrIsEmpty = false;

                    LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch1 = controllerContext.partitionLeadershipInfo.get(topicAndPartition);
                    if (leaderIsrAndControllerEpoch1 == null) {
                        leaderAndIsrIsEmpty = true;
                    } else {
                        LeaderIsrAndControllerEpoch updatedLeaderIsrAndControllerEpoch = controller.removeReplicaFromIsr(topic, partition, replicaId);

                        if (updatedLeaderIsrAndControllerEpoch == null) {
                            leaderAndIsrIsEmpty = true;
                        } else {
                            // send the shrunk ISR state change request only to the leader
                            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(Lists.newArrayList(updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader),
                                    topic, partition, updatedLeaderIsrAndControllerEpoch, Sets.newHashSet(replicaAssignment));
                            replicaState.put(Tuple3.make(topic, partition, replicaId), ReplicaState.OfflineReplica);
                            stateChangeLogger.trace("Controller {} epoch {} changed state of replica {} for partition {} to OfflineReplica",
                                    controllerId, controller.epoch(), replicaId, topicAndPartition);
                            leaderAndIsrIsEmpty = false;
                        }

                        if (leaderAndIsrIsEmpty)
                            throw new StateChangeFailedException(
                                    "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty",
                                    replicaId, topicAndPartition);
                    }
            }
        } catch (Throwable t) {
            stateChangeLogger.error("Controller {} epoch {} initiated state change of replica {} for partition [{},{}] to {} failed",
                    controllerId, controller.epoch(), replicaId, topic, partition, targetState, t);
        }
    }

    private void assertValidPreviousStates(String topic, int partition, int replicaId, List<ReplicaState> fromStates,
                                           ReplicaState targetState) {
        boolean contains = fromStates.contains(replicaState.get(Tuple3.make(topic, partition, replicaId)));
        if (!contains) {
            throw new KafkaException(String.format("Replica %s for partition [%s,%d] should be in the %s states before moving to %s state",
                    replicaId, topic, partition, fromStates, targetState) +
                    String.format(". Instead it is in %s state", replicaState.get(Tuple3.make(topic, partition, replicaId))));
        }
    }


    private void registerBrokerChangeListener() {
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, new BrokerChangeListener());
    }

    /**
     * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
     * in zookeeper
     */
    private void initializeReplicaState() {
        for (Map.Entry<TopicAndPartition, Integer> entry : controllerContext.partitionReplicaAssignment.entries()) {
            TopicAndPartition topicPartition = entry.getKey();
            Integer replicaId = entry.getValue();

            String topic = topicPartition.topic;
            int partition = topicPartition.partition;


            boolean contains = controllerContext.liveBrokerIds().contains(replicaId);

            replicaState.put(Tuple3.make(topic, partition, replicaId),
                    contains ? ReplicaState.OnlineReplica : ReplicaState.OfflineReplica);
        }
    }

    private Set<PartitionAndReplica> getAllReplicasOnBroker(final Set<String> topics, final Set<Integer> brokerIds) {
        final Set<PartitionAndReplica> ret = Sets.newHashSet();

        Utils.foreach(brokerIds, new Callable1<Integer>() {
            @Override
            public void apply(final Integer brokerId) {
                Multimap<TopicAndPartition, Integer> partitionsAssignedToThisBroker = Utils.filter(controllerContext.partitionReplicaAssignment, new Predicate2<TopicAndPartition, Collection<Integer>>() {
                    @Override
                    public boolean apply(TopicAndPartition topicAndPartition, Collection<Integer> integers) {
                        return topics.contains(topicAndPartition.topic) && integers.contains(brokerId);
                    }
                });

                if (partitionsAssignedToThisBroker.size() == 0)
                    logger.info("No state transitions triggered since no partitions are assigned to brokers {}", brokerIds);

                Utils.foreach(partitionsAssignedToThisBroker, new Callable2<TopicAndPartition, Collection<Integer>>() {
                    @Override
                    public void apply(TopicAndPartition _1, Collection<Integer> integers) {
                        ret.add(new PartitionAndReplica(_1.topic, _1.partition, brokerId));
                    }
                });
            }
        });

        return ret;
    }

    public List<TopicAndPartition> getPartitionsAssignedToBroker(List<String> topics, final int brokerId) {
        return Lists.newArrayList(Utils.filter(controllerContext.partitionReplicaAssignment, new Predicate2<TopicAndPartition, Collection<Integer>>() {

            @Override
            public boolean apply(TopicAndPartition topicAndPartition, Collection<Integer> integers) {
                return integers.contains(brokerId);
            }
        }).keySet());
    }

    /**
     * This is the zookeeper listener that triggers all the state transitions for a replica
     */
    class BrokerChangeListener implements IZkChildListener {
        Logger logger = LoggerFactory.getLogger(BrokerChangeListener.class + "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: ");

        @Override
        public void handleChildChange(String parentPath, final List<String> currentBrokerList) throws Exception {
            logger.info("Broker change listener fired for path {} with children {}", parentPath, currentBrokerList);
            synchronized (controllerContext.controllerLock) {
                if (hasStarted.get()) {
                    ControllerStats.instance.leaderElectionTimer.time(new Function0<Object>() {
                        @Override
                        public Object apply() {
                            try {
                                Set<Integer> curBrokerIds = Utils.mapSet(currentBrokerList, new Function1<String, Integer>() {
                                    @Override
                                    public Integer apply(String arg) {
                                        return Integer.parseInt(arg);
                                    }
                                });
                                Set<Integer> newBrokerIds = Sets.newHashSet(curBrokerIds);
                                newBrokerIds.removeAll(controllerContext.liveOrShuttingDownBrokerIds());
                                List<Broker> newBrokerInfo = Utils.mapList(newBrokerIds, new Function1<Integer, Broker>() {
                                    @Override
                                    public Broker apply(Integer _) {
                                        return ZkUtils.getBrokerInfo(zkClient, _);
                                    }
                                });

                                List<Broker> newBrokers = Utils.filter(newBrokerInfo, new Predicate<Broker>() {
                                    @Override
                                    public boolean apply(Broker _) {
                                        return _ != null;
                                    }
                                });

                                Set<Integer> deadBrokerIds = Sets.newHashSet(controllerContext.liveOrShuttingDownBrokerIds());
                                deadBrokerIds.removeAll(curBrokerIds);

                                controllerContext.liveBrokers(Sets.newHashSet(Utils.filter(
                                        Utils.mapList(curBrokerIds, new Function1<Integer, Broker>() {
                                            @Override
                                            public Broker apply(Integer _) {
                                                return ZkUtils.getBrokerInfo(zkClient, _);
                                            }
                                        }), new Predicate<Broker>() {
                                    @Override
                                    public boolean apply(Broker _) {
                                        return _ != null;
                                    }
                                })));

                                // curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get));
                                logger.info("Newly added brokers: {}, deleted brokers: {}, all live brokers: {}",
                                        newBrokerIds, deadBrokerIds, controllerContext.liveBrokerIds());
                                Utils.foreach(newBrokers, new Callable1<Broker>() {
                                    @Override
                                    public void apply(Broker _) {
                                        controllerContext.controllerChannelManager.addBroker(_);
                                    }
                                });
                                Utils.foreach(deadBrokerIds, new Callable1<Integer>() {
                                    @Override
                                    public void apply(Integer _) {
                                        controllerContext.controllerChannelManager.removeBroker(_);
                                    }
                                });

                                if (newBrokerIds.size() > 0)
                                    controller.onBrokerStartup(newBrokerIds);
                                if (deadBrokerIds.size() > 0)
                                    controller.onBrokerFailure(Lists.newArrayList(deadBrokerIds));
                            } catch (Throwable e) {
                                logger.error("Error while handling broker changes", e);
                            }

                            return null;
                        }
                    });
                }
            }
        }
    }
}
