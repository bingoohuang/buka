package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.yammer.metrics.core.Gauge;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.api.LeaderAndIsr;
import kafka.api.RequestOrResponse;
import kafka.common.*;
import kafka.metrics.KafkaMetricsGroup;
import kafka.server.KafkaConfig;
import kafka.server.ZookeeperLeaderElector;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static kafka.utils.ZkUtils.*;

public class KafkaController extends KafkaMetricsGroup implements KafkaControllerMBean {
    public KafkaConfig config;
    public ZkClient zkClient;

    public KafkaController(KafkaConfig config, ZkClient zkClient) {
        this.config = config;
        this.zkClient = zkClient;

        init();
    }

    private void init() {
        logger = LoggerFactory.getLogger(KafkaController.class + "[Controller " + config.brokerId + "]: ");
        controllerContext = new ControllerContext(zkClient, config.zkSessionTimeoutMs);
        controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, new Callable0() {
            @Override
            public void apply() {
                onControllerFailover();
            }
        }, config.brokerId);

        offlinePartitionSelector = new OfflinePartitionLeaderSelector(controllerContext);
        reassignedPartitionLeaderSelector = new ReassignedPartitionLeaderSelector(controllerContext);
        preferredReplicaPartitionLeaderSelector = new PreferredReplicaPartitionLeaderSelector(controllerContext);
        controlledShutdownPartitionLeaderSelector = new ControlledShutdownLeaderSelector(controllerContext);
        brokerRequestBatch = new ControllerBrokerRequestBatch(controllerContext, new Callable3<Integer, RequestOrResponse, Callable1<RequestOrResponse>>() {
            @Override
            public void apply(Integer integer, RequestOrResponse requestOrResponse, Callable1<RequestOrResponse> requestOrResponseCallable1) {
                sendRequest(integer, requestOrResponse, requestOrResponseCallable1);
            }
        }, this.config.brokerId, this.clientId());

        registerControllerChangedListener();

        newGauge("ActiveControllerCount", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return isActive() ? 1 : 0;
            }
        });

        newGauge("OfflinePartitionsCount", new Gauge<Integer>() {
            @Override
            public Integer value() {
                synchronized (controllerContext.controllerLock) {
                    if (!isActive()) return 0;
                    return Utils.foldLeft(controllerContext.partitionLeadershipInfo, 0, new Function3<Integer, TopicAndPartition, LeaderIsrAndControllerEpoch, Integer>() {
                        @Override
                        public Integer apply(Integer arg1, TopicAndPartition arg2, LeaderIsrAndControllerEpoch _2) {
                            return arg1 + (!controllerContext.liveOrShuttingDownBrokerIds().contains(_2.leaderAndIsr.leader) ? 1 : 0);
                        }
                    });
                }
            }
        });


        newGauge("PreferredReplicaImbalanceCount", new Gauge<Integer>() {
            @Override
            public Integer value() {
                synchronized (controllerContext.controllerLock) {
                    if (!isActive()) return 0;

                    return Utils.foldLeft(controllerContext.partitionReplicaAssignment, 0, new Function3<Integer, TopicAndPartition, Collection<Integer>, Integer>() {
                        @Override
                        public Integer apply(Integer arg1, TopicAndPartition topicPartition, Collection<Integer> replicas) {
                            return arg1 + (controllerContext.partitionLeadershipInfo.get(topicPartition).leaderAndIsr.leader != Utils.head(replicas) ? 1 : 0);
                        }
                    });
                }
            }
        });
    }

    Logger logger;

    private boolean isRunning = true;
    private Logger stateChangeLogger = LoggerFactory.getLogger(KafkaControllers.stateChangeLogger);
    public ControllerContext controllerContext;
    private PartitionStateMachine partitionStateMachine = new PartitionStateMachine(this);
    private ReplicaStateMachine replicaStateMachine = new ReplicaStateMachine(this);
    private ZookeeperLeaderElector controllerElector;
    // have a separate scheduler for the controller to be able to start and stop independently of the
    // kafka server
    private KafkaScheduler autoRebalanceScheduler = new KafkaScheduler(1);
    public OfflinePartitionLeaderSelector offlinePartitionSelector;
    private ReassignedPartitionLeaderSelector reassignedPartitionLeaderSelector;
    private PreferredReplicaPartitionLeaderSelector preferredReplicaPartitionLeaderSelector;
    private ControlledShutdownLeaderSelector controlledShutdownPartitionLeaderSelector;
    private ControllerBrokerRequestBatch brokerRequestBatch;


    public int epoch() {
        return controllerContext.epoch;
    }

    public String clientId() {
        return String.format("id_%d-host_%s-port_%d", config.brokerId, config.hostName, config.port);
    }

    /**
     * On clean shutdown, the controller first determines the partitions that the
     * shutting down broker leads, and moves leadership of those partitions to another broker
     * that is in that partition's ISR.
     *
     * @param id Id of the broker to shutdown.
     * @return The number of partitions that the broker still leads.
     */
    @Override
    public Set<TopicAndPartition> shutdownBroker(final int id) {
        if (!isActive()) {
            throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown");
        }

        synchronized (controllerContext.brokerShutdownLock) {
            logger.info("Shutting down broker " + id);

            synchronized (controllerContext.controllerLock) {
                if (!controllerContext.liveOrShuttingDownBrokerIds().contains(id))
                    throw new BrokerNotAvailableException("Broker id %d does not exist.", id);

                controllerContext.shuttingDownBrokerIds.add(id);

                logger.debug("All shutting down brokers: {}", controllerContext.shuttingDownBrokerIds);
                logger.debug("Live brokers:{} ", controllerContext.liveBrokerIds());
            }

            Map<TopicAndPartition, Integer> allPartitionsAndReplicationFactorOnBroker;
            synchronized (controllerContext.controllerLock) {
                allPartitionsAndReplicationFactorOnBroker = Utils.map(getPartitionsAssignedToBroker(zkClient, Lists.newArrayList(controllerContext.allTopics), id), new Function1<Tuple2<String, Integer>, Tuple2<TopicAndPartition, Integer>>() {
                    @Override
                    public Tuple2<TopicAndPartition, Integer> apply(Tuple2<String, Integer> _) {
                        String topic = _._1;
                        int partition = _._2;
                        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

                        return Tuple2.make(topicAndPartition, controllerContext.partitionReplicaAssignment.get(topicAndPartition).size());
                    }
                });
            }

            Utils.foreach(allPartitionsAndReplicationFactorOnBroker, new Callable2<TopicAndPartition, Integer>() {
                @Override
                public void apply(TopicAndPartition topicAndPartition, Integer replicationFactor) {
                    // Move leadership serially to relinquish lock.
                    synchronized (controllerContext.controllerLock) {
                        LeaderIsrAndControllerEpoch currLeaderIsrAndControllerEpoch = controllerContext.partitionLeadershipInfo.get(topicAndPartition);

                        if (currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id) {
                            // If the broker leads the topic partition, transition the leader and update isr. Updates zk and
                            // notifies all affected brokers
                            partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), PartitionState.OnlinePartition,
                                    controlledShutdownPartitionLeaderSelector);
                        } else {
                            // Stop the replica first. The state change below initiates ZK changes which should take some time
                            // before which the stop replica request should be completed (in most cases)
                            brokerRequestBatch.newBatch();
                            brokerRequestBatch.addStopReplicaRequestForBrokers(Lists.newArrayList(id), topicAndPartition.topic, topicAndPartition.partition, /*deletePartition = */false);
                            brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());

                            // If the broker is a follower, updates the isr in ZK and notifies the current leader
                            replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic,
                                    topicAndPartition.partition, id)), ReplicaState.OfflineReplica);
                        }
                    }
                }
            });


            final Set<TopicAndPartition> ret = Sets.newHashSet();
            synchronized (controllerContext.controllerLock) {
                logger.trace("All leaders = {}", controllerContext.partitionLeadershipInfo);


                Utils.foreach(Utils.filter(controllerContext.partitionLeadershipInfo, new Predicate<Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch>>() {
                    @Override
                    public boolean apply(Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch> input) {
                        TopicAndPartition topicAndPartition = input.getKey();
                        LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = input.getValue();
                        return leaderIsrAndControllerEpoch.leaderAndIsr.leader == id && controllerContext.partitionReplicaAssignment.get(topicAndPartition).size() > 1;
                    }
                }), new Callable2<TopicAndPartition, LeaderIsrAndControllerEpoch>() {
                    @Override
                    public void apply(TopicAndPartition _1, LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch) {
                        ret.add(_1);
                    }
                });
            }

            return ret;
        }
    }


    /**
     * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
     * It does the following things on the become-controller state change -
     * 1. Register controller epoch changed listener
     * 2. Increments the controller epoch
     * 3. Initializes the controller's context object that holds cache objects for current topics, live brokers and
     * leaders for all existing partitions.
     * 4. Starts the controller's channel manager
     * 5. Starts the replica state machine
     * 6. Starts the partition state machine
     * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
     * This ensures another controller election will be triggered and there will always be an actively serving controller
     */
    public void onControllerFailover() {
        if (isRunning) {
            logger.info("Broker {} starting become controller state transition", config.brokerId);
            // increment the controller epoch
            incrementControllerEpoch(zkClient);
            // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
            registerReassignedPartitionsListener();
            registerPreferredReplicaElectionListener();
            partitionStateMachine.registerListeners();
            replicaStateMachine.registerListeners();
            initializeControllerContext();
            replicaStateMachine.startup();
            partitionStateMachine.startup();
            // register the partition change listeners for all existing topics on failover
            Utils.foreach(controllerContext.allTopics, new Callable1<String>() {
                @Override
                public void apply(String topic) {
                    partitionStateMachine.registerPartitionChangeListener(topic);
                }
            });
            Utils.registerMBean(this, KafkaControllers.MBeanName);
            logger.info("Broker {} is ready to serve as the new controller with epoch {}", config.brokerId, epoch());
            initializeAndMaybeTriggerPartitionReassignment();
            initializeAndMaybeTriggerPreferredReplicaElection();
      /* send partition leadership info to all live brokers */
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds());
            if (config.autoLeaderRebalanceEnable) {
                logger.info("starting the partition rebalance scheduler");
                autoRebalanceScheduler.startup();
                autoRebalanceScheduler.schedule("partition-rebalance-thread", new Runnable() {
                    @Override
                    public void run() {
                        checkAndTriggerPartitionRebalance();
                    }
                },
                        5, config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS);
            }
        } else
            logger.info("Controller has been shut down, aborting startup/failover");
    }

    /**
     * Returns true if this broker is the current controller.
     */
    public boolean isActive() {
        synchronized (controllerContext.controllerLock) {
            return controllerContext.controllerChannelManager != null;
        }
    }


    /**
     * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
     * brokers as input. It does the following -
     * 1. Triggers the OnlinePartition state change for all new/offline partitions
     * 2. It checks whether there are reassigned replicas assigned to any newly started brokers.  If
     * so, it performs the reassignment logic for each topic/partition.
     * <p/>
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
     * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
     * partitions currently new or offline (rather than every partition this controller is aware of)
     * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
     * every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
     */
    public void onBrokerStartup(final Set<Integer> newBrokersSet) {
        logger.info("New broker startup callback for {}", newBrokersSet);

        // send update metadata request for all partitions to the newly restarted brokers. In cases of controlled shutdown
        // leaders will not be elected when a new broker comes up. So at least in the common controlled shutdown case, the
        // metadata will reach the new brokers faster
        sendUpdateMetadataRequest(newBrokersSet);
        // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
        // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
        replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, Lists.newArrayList(controllerContext.allTopics),
                Lists.newArrayList(newBrokersSet)), ReplicaState.OnlineReplica);
        // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
        // to see if these brokers can become leaders for some/all of those
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // check if reassignment of some partitions need to be restarted
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsWithReplicasOnNewBrokers = Utils.filter(controllerContext.partitionsBeingReassigned,
                new Predicate<Map.Entry<TopicAndPartition, ReassignedPartitionsContext>>() {
                    @Override
                    public boolean apply(Map.Entry<TopicAndPartition, ReassignedPartitionsContext> input) {
                        return Utils.exists(input.getValue().newReplicas, new Function1<Integer, Boolean>() {
                            @Override
                            public Boolean apply(Integer _) {
                                return newBrokersSet.contains(_);
                            }
                        });
                    }
                });

        Utils.foreach(partitionsWithReplicasOnNewBrokers, new Callable2<TopicAndPartition, ReassignedPartitionsContext>() {
            @Override
            public void apply(TopicAndPartition _1, ReassignedPartitionsContext _2) {
                onPartitionReassignment(_1, _2);
            }
        });

    }

    /**
     * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Mark partitions with dead leaders as offline
     * 2. Triggers the OnlinePartition state change for all new/offline partitions
     * 3. Invokes the OfflineReplica state change on the input list of newly started brokers
     * <p/>
     * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point.  This is because
     * the partition state machine will refresh our cache for us when performing leader election for all new/offline
     * partitions coming online.
     */
    public void onBrokerFailure(List<Integer> deadBrokers) {
        logger.info("Broker failure callback for {}", deadBrokers);

        List<Integer> deadBrokersThatWereShuttingDown = Utils.filter(deadBrokers, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer id) {
                return controllerContext.shuttingDownBrokerIds.remove(id);
            }
        });
        logger.info("Removed {} from list of shutting down brokers.", deadBrokersThatWereShuttingDown);

        final Set<Integer> deadBrokersSet = Sets.newHashSet(deadBrokers);
        // trigger OfflinePartition state for all partitions whose current leader is one amongst the dead brokers
        Set<TopicAndPartition> partitionsWithoutLeader =
                Utils.filter(controllerContext.partitionLeadershipInfo,
                        new Predicate<Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch>>() {
                            @Override
                            public boolean apply(Map.Entry<TopicAndPartition, LeaderIsrAndControllerEpoch> partitionAndLeader) {
                                return deadBrokersSet.contains(partitionAndLeader.getValue().leaderAndIsr.leader);
                            }
                        }).keySet();

        partitionStateMachine.handleStateChanges(partitionsWithoutLeader, PartitionState.OfflinePartition);
        // trigger OnlinePartition state changes for offline or new partitions
        partitionStateMachine.triggerOnlinePartitionStateChange();
        // handle dead replicas
        replicaStateMachine.handleStateChanges(getAllReplicasOnBroker(zkClient, Lists.newArrayList(controllerContext.allTopics), deadBrokers),
                ReplicaState.OfflineReplica);
    }

    /**
     * This callback is invoked by the partition state machine's topic change listener with the list of failed brokers
     * as input. It does the following -
     * 1. Registers partition change listener. This is not required until KAFKA-347
     * 2. Invokes the new partition callback
     */
    public void onNewTopicCreation(Set<String> topics, Set<TopicAndPartition> newPartitions) {
        logger.info("New topic creation callback for {}", newPartitions);
        // subscribe to partition changes
        Utils.foreach(topics, new Callable1<String>() {
            @Override
            public void apply(String topic) {
                partitionStateMachine.registerPartitionChangeListener(topic);
            }
        });
        onNewPartitionCreation(newPartitions);
    }

    /**
     * This callback is invoked by the topic change callback with the list of failed brokers as input.
     * It does the following -
     * 1. Move the newly created partitions to the NewPartition state
     * 2. Move the newly created partitions from NewPartition->OnlinePartition state
     */
    public void onNewPartitionCreation(Set<TopicAndPartition> newPartitions) {
        logger.info("New partition creation callback for {}", newPartitions);
        partitionStateMachine.handleStateChanges(newPartitions, PartitionState.NewPartition);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), ReplicaState.NewReplica);
        partitionStateMachine.handleStateChanges(newPartitions, PartitionState.OnlinePartition, offlinePartitionSelector);
        replicaStateMachine.handleStateChanges(getAllReplicasForPartition(newPartitions), ReplicaState.OnlineReplica);
    }


    /**
     * This callback is invoked by the reassigned partitions listener. When an admin command initiates a partition
     * reassignment, it creates the /admin/reassign_partitions path that triggers the zookeeper listener.
     * Reassigning replicas for a partition goes through a few stages -
     * RAR = Reassigned replicas
     * AR = Original list of replicas for partition
     * 1. Write new AR = AR + RAR. At this time, update the leader epoch in zookeeper and send a LeaderAndIsr request with
     * AR = AR + RAR to all replicas in (AR + RAR)
     * 2. Start new replicas RAR - AR.
     * 3. Wait until new replicas are in sync with the leader
     * 4. If the leader is not in RAR, elect a new leader from RAR. If new leader needs to be elected from RAR, a LeaderAndIsr
     * will be sent. If not, then leader epoch will be incremented in zookeeper and a LeaderAndIsr request will be sent.
     * In any case, the LeaderAndIsr request will have AR = RAR. This will prevent the leader from adding any replica in
     * RAR - AR back in the ISR
     * 5. Stop old replicas AR - RAR. As part of this, we make 2 state changes OfflineReplica and NonExistentReplica. As part
     * of OfflineReplica state change, we shrink the ISR to remove RAR - AR in zookeeper and sent a LeaderAndIsr ONLY to
     * the Leader to notify it of the shrunk ISR. After that, we send a StopReplica (delete = false) to the replicas in
     * RAR - AR. Currently, NonExistentReplica state change is a NO-OP
     * 6. Write new AR = RAR. As part of this, we finally change the AR in zookeeper to RAR.
     * 7. Remove partition from the /admin/reassign_partitions path
     */
    public void onPartitionReassignment(final TopicAndPartition topicAndPartition, ReassignedPartitionsContext reassignedPartitionContext) {
        Collection<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;

        boolean areReplicasInIsr = areReplicasInIsr(topicAndPartition.topic, topicAndPartition.partition, reassignedReplicas);

        if (areReplicasInIsr) {
            Set<Integer> oldReplicas = Sets.newHashSet(controllerContext.partitionReplicaAssignment.get(topicAndPartition));
            oldReplicas.removeAll(reassignedReplicas);
            // mark the new replicas as online
            Utils.foreach(reassignedReplicas, new Callable1<Integer>() {
                @Override
                public void apply(Integer replica) {
                    replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition,
                            replica)), ReplicaState.OnlineReplica);
                }
            });

            // check if current leader is in the new replicas list. If not, controller needs to trigger leader election
            moveReassignedPartitionLeaderIfRequired(topicAndPartition, reassignedPartitionContext);
            // stop older replicas
            stopOldReplicasOfReassignedPartition(topicAndPartition, reassignedPartitionContext, oldReplicas);
            // write the new list of replicas for this partition in zookeeper
            updateAssignedReplicasForPartition(topicAndPartition, Lists.newArrayList(reassignedReplicas));
            // update the /admin/reassign_partitions path to remove this partition
            removePartitionFromReassignedPartitions(topicAndPartition);
            logger.info("Removed partition {} from the list of reassigned partitions in zookeeper", topicAndPartition);
            controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
            // after electing leader, the replicas and isr information changes, so resend the update metadata request
            sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds(), Sets.newHashSet(topicAndPartition));
        } else {
            logger.info("New replicas {} for partition {} being reassigned not yet caught up with the leader", reassignedReplicas, topicAndPartition);
            Set<Integer> newReplicasNotInOldReplicaList = Sets.newHashSet(reassignedReplicas);
            newReplicasNotInOldReplicaList.removeAll(controllerContext.partitionReplicaAssignment.get(topicAndPartition));
            Set<Integer> newAndOldReplicas = Sets.newHashSet(reassignedPartitionContext.newReplicas);
            newAndOldReplicas.addAll(controllerContext.partitionReplicaAssignment.get(topicAndPartition));

            // write the expanded list of replicas to zookeeper
            updateAssignedReplicasForPartition(topicAndPartition, Lists.newArrayList(newAndOldReplicas));
            // update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
            updateLeaderEpochAndSendRequest(topicAndPartition, Lists.newArrayList(controllerContext.partitionReplicaAssignment.get(topicAndPartition)),
                    Sets.newHashSet(newAndOldReplicas));
            // start new replicas
            startNewReplicasForReassignedPartition(topicAndPartition, reassignedPartitionContext, newReplicasNotInOldReplicaList);
            logger.info("Waiting for new replicas {} for partition {} being reassigned to catch up with the leader", reassignedReplicas, topicAndPartition);
        }
    }

    private void watchIsrChangesForReassignedPartition(String topic,
                                                       int partition,
                                                       ReassignedPartitionsContext reassignedPartitionContext) {
        Collection<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        ReassignedPartitionsIsrChangeListener isrChangeListener = new ReassignedPartitionsIsrChangeListener(this, topic, partition,
                Sets.newHashSet(reassignedReplicas));
        reassignedPartitionContext.isrChangeListener = isrChangeListener;
        // register listener on the leader and isr path to wait until they catch up with the current leader
        zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), isrChangeListener);
    }

    public void initiateReassignReplicasForTopicPartition(TopicAndPartition topicAndPartition,
                                                          ReassignedPartitionsContext reassignedPartitionContext) {
        Collection<Integer> newReplicas = reassignedPartitionContext.newReplicas;
        String topic = topicAndPartition.topic;
        int partition = topicAndPartition.partition;
        List<Integer> aliveNewReplicas = Utils.filter(newReplicas, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer r) {
                return controllerContext.liveBrokerIds().contains(r);
            }
        });
        try {
            Collection<Integer> assignedReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
            if (assignedReplicas != null) {
                throw new KafkaException("Attempt to reassign partition %s that doesn't exist", topicAndPartition);
            }

            if (assignedReplicas == newReplicas) {
                throw new KafkaException("Partition %s to be reassigned is already assigned to replicas %s. Ignoring request for partition reassignment",
                        topicAndPartition, newReplicas);
            } else {
                if (aliveNewReplicas == newReplicas) {
                    logger.info("Handling reassignment of partition {} to new replicas {}", topicAndPartition, newReplicas);
                    // first register ISR change listener
                    watchIsrChangesForReassignedPartition(topic, partition, reassignedPartitionContext);
                    controllerContext.partitionsBeingReassigned.put(topicAndPartition, reassignedPartitionContext);
                    onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                } else {
                    // some replica in RAR is not alive. Fail partition reassignment
                    throw new KafkaException("Only %s replicas out of the new set of replicas %s for partition %s to be reassigned are alive. Failing partition reassignment",
                            aliveNewReplicas, newReplicas, topicAndPartition);
                }
            }

        } catch (Throwable e) {
            logger.error("Error completing reassignment of partition {}", topicAndPartition, e);
            // remove the partition from the admin path to unblock the admin client
            removePartitionFromReassignedPartitions(topicAndPartition);
        }


    }

    public void onPreferredReplicaElection(Set<TopicAndPartition> partitions) {
        logger.info("Starting preferred replica leader election for partitions {}", partitions);
        try {
            controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitions);
            partitionStateMachine.handleStateChanges(partitions, PartitionState.OnlinePartition, preferredReplicaPartitionLeaderSelector);
        } catch (Throwable e) {
            logger.error("Error completing preferred replica leader election for partitions {}", partitions, e);
        } finally {
            removePartitionsFromPreferredReplicaElection(partitions);
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
     * is the controller. It merely registers the session expiration listener and starts the controller leader
     * elector
     */
    public void startup() {
        synchronized (controllerContext.controllerLock) {
            logger.info("Controller starting up");
            registerSessionExpirationListener();
            isRunning = true;
            controllerElector.startup();
            logger.info("Controller startup complete");
        }
    }

    /**
     * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
     * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
     * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
     */
    public void shutdown() {
        synchronized (controllerContext.controllerLock) {
            isRunning = false;
            partitionStateMachine.shutdown();
            replicaStateMachine.shutdown();
            if (config.autoLeaderRebalanceEnable)
                autoRebalanceScheduler.shutdown();
            if (controllerContext.controllerChannelManager != null) {
                controllerContext.controllerChannelManager.shutdown();
                controllerContext.controllerChannelManager = null;
                logger.info("Controller shutdown complete");
            }
        }
    }

    public void sendRequest(int brokerId, RequestOrResponse request) {
        sendRequest(brokerId, request, null);
    }

    public void sendRequest(int brokerId, RequestOrResponse request, Callable1<RequestOrResponse> callback /*= null*/) {
        controllerContext.controllerChannelManager.sendRequest(brokerId, request, callback);
    }

    public void incrementControllerEpoch(ZkClient zkClient) {
        try {
            int newControllerEpoch = controllerContext.epoch + 1;
            Tuple2<Boolean, Integer> result = ZkUtils.conditionalUpdatePersistentPathIfExists(zkClient,
                    ZkUtils.ControllerEpochPath, newControllerEpoch + "", controllerContext.epochZkVersion);
            boolean updateSucceeded = result._1;
            Integer newVersion = result._2;
            if (!updateSucceeded)
                throw new ControllerMovedException("Controller moved to another broker. Aborting controller startup procedure");
            else {
                controllerContext.epochZkVersion = newVersion;
                controllerContext.epoch = newControllerEpoch;
            }
        } catch (ZkNoNodeException e) {
            // if path doesn't exist, this is the first controller whose epoch should be 1
            // the following call can still fail if another controller gets elected between checking if the path exists and
            // trying to create the controller epoch path
            try {
                zkClient.createPersistent(ZkUtils.ControllerEpochPath, KafkaControllers.InitialControllerEpoch + "");
                controllerContext.epoch = KafkaControllers.InitialControllerEpoch;
                controllerContext.epochZkVersion = KafkaControllers.InitialControllerEpochZkVersion;
            } catch (ZkNodeExistsException e1) {
                throw new ControllerMovedException("Controller moved to another broker. " +
                        "Aborting controller startup procedure");
            } catch (Throwable oe) {
                logger.error("Error while incrementing controller epoch", oe);
            }
        } catch (Throwable e) {
            logger.error("Error while incrementing controller epoch", e);

        }
        logger.info("Controller {} incremented epoch to {}", config.brokerId, controllerContext.epoch);
    }


    private void registerSessionExpirationListener() {
        zkClient.subscribeStateChanges(new SessionExpirationListener());
    }


    private void initializeControllerContext() {
        controllerContext.liveBrokers(Sets.newHashSet(ZkUtils.getAllBrokersInCluster(zkClient)));
        controllerContext.allTopics = ZkUtils.getAllTopics(zkClient);
        controllerContext.partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(controllerContext.allTopics));
        controllerContext.partitionLeadershipInfo = Maps.newHashMap();
        controllerContext.shuttingDownBrokerIds = Sets.newHashSet();
        // update the leader and isr cache for all existing partitions from Zookeeper
        updateLeaderAndIsrCache();
        // start the channel manager
        startChannelManager();
        logger.info("Currently active brokers in the cluster: {}", controllerContext.liveBrokerIds());
        logger.info("Currently shutting brokers in the cluster: {}", controllerContext.shuttingDownBrokerIds);
        logger.info("Current list of topics in the cluster: {}", controllerContext.allTopics);
    }

    private void initializeAndMaybeTriggerPartitionReassignment() {
        // read the partitions being reassigned from zookeeper path /admin/reassign_partitions
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // check if they are already completed
        Set<TopicAndPartition> reassignedPartitions = Utils.filter(partitionsBeingReassigned, new Predicate2<TopicAndPartition, ReassignedPartitionsContext>() {
            @Override
            public boolean apply(TopicAndPartition _1, ReassignedPartitionsContext _2) {
                return controllerContext.partitionReplicaAssignment.get(_1).equals(_2.newReplicas);
            }
        }).keySet();

        Utils.foreach(reassignedPartitions, new Callable1<TopicAndPartition>() {
            @Override
            public void apply(TopicAndPartition p) {
                removePartitionFromReassignedPartitions(p);
            }
        });

        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsToReassign = Maps.newHashMap();
        partitionsToReassign.putAll(partitionsBeingReassigned);
        Utils.removeAll(partitionsToReassign, reassignedPartitions);

        logger.info("Partitions being reassigned: {}", partitionsBeingReassigned.toString());
        logger.info("Partitions already reassigned: {}", reassignedPartitions.toString());
        logger.info("Resuming reassignment of partitions: {}", partitionsToReassign.toString());

        Utils.foreach(partitionsToReassign, new Callable2<TopicAndPartition, ReassignedPartitionsContext>() {
            @Override
            public void apply(TopicAndPartition _1, ReassignedPartitionsContext _2) {
                initiateReassignReplicasForTopicPartition(_1, _2);
            }
        });
    }


    private void initializeAndMaybeTriggerPreferredReplicaElection() {
        // read the partitions undergoing preferred replica election from zookeeper path
        Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection = ZkUtils.getPartitionsUndergoingPreferredReplicaElection(zkClient);
        // check if they are already completed
        List<TopicAndPartition> partitionsThatCompletedPreferredReplicaElection = Utils.filter(partitionsUndergoingPreferredReplicaElection, new Predicate<TopicAndPartition>() {
            @Override
            public boolean apply(TopicAndPartition partition) {
                return controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader == Utils.head(controllerContext.partitionReplicaAssignment.get(partition));
            }
        });

        controllerContext.partitionsUndergoingPreferredReplicaElection.addAll(partitionsUndergoingPreferredReplicaElection);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsThatCompletedPreferredReplicaElection);
        logger.info("Partitions undergoing preferred replica election: {}", partitionsUndergoingPreferredReplicaElection);
        logger.info("Partitions that completed preferred replica election: {}", partitionsThatCompletedPreferredReplicaElection);
        logger.info("Resuming preferred replica election for partitions: {}", controllerContext.partitionsUndergoingPreferredReplicaElection);
        onPreferredReplicaElection(controllerContext.partitionsUndergoingPreferredReplicaElection);
    }


    private void startChannelManager() {
        controllerContext.controllerChannelManager = new ControllerChannelManager(controllerContext, config);
        controllerContext.controllerChannelManager.startup();
    }

    private void updateLeaderAndIsrCache() {
        Map<TopicAndPartition, LeaderIsrAndControllerEpoch> leaderAndIsrInfo = ZkUtils.getPartitionLeaderAndIsrForTopics(zkClient, controllerContext.partitionReplicaAssignment.keySet());
        Utils.foreach(leaderAndIsrInfo, new Callable2<TopicAndPartition, LeaderIsrAndControllerEpoch>() {
            @Override
            public void apply(TopicAndPartition topicPartition, LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch) {
                controllerContext.partitionLeadershipInfo.put(topicPartition, leaderIsrAndControllerEpoch);
            }
        });
    }

    private boolean areReplicasInIsr(String topic, int partition, Collection<Integer> replicas) {
        final LeaderAndIsr leaderAndIsr = getLeaderAndIsrForPartition(zkClient, topic, partition);
        if (leaderAndIsr != null) {
            List<Integer> replicasNotInIsr = Utils.filter(replicas, new Predicate<Integer>() {
                @Override
                public boolean apply(Integer r) {
                    return !leaderAndIsr.isr.contains(r);
                }
            });
            return replicasNotInIsr.isEmpty();
        } else {
            return false;
        }
    }


    private void moveReassignedPartitionLeaderIfRequired(TopicAndPartition topicAndPartition,
                                                         ReassignedPartitionsContext reassignedPartitionContext) {
        Collection<Integer> reassignedReplicas = reassignedPartitionContext.newReplicas;
        int currentLeader = controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.leader;
        // change the assigned replica list to just the reassigned replicas in the cache so it gets sent out on the LeaderAndIsr
        // request to the current or new leader. This will prevent it from adding the old replicas to the ISR
        Collection<Integer> oldAndNewReplicas = controllerContext.partitionReplicaAssignment.get(topicAndPartition);
        controllerContext.partitionReplicaAssignment.putAll(topicAndPartition, reassignedReplicas);
        if (!reassignedPartitionContext.newReplicas.contains(currentLeader)) {
            logger.info("Leader {} for partition {} being reassigned, is not in the new list of replicas {}. Re-electing leader", currentLeader, topicAndPartition,
                    reassignedReplicas);
            // move the leader to one of the alive and caught up new replicas
            partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), PartitionState.OnlinePartition, reassignedPartitionLeaderSelector);
        } else {
            // check if the leader is alive or not
            if (controllerContext.liveBrokerIds().contains(currentLeader)) {
                logger.info("Leader {} for partition {} being reassigned, is already in the new list of replicas {} and is alive", currentLeader, topicAndPartition,
                        reassignedReplicas);
                // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
                updateLeaderEpochAndSendRequest(topicAndPartition, Lists.newArrayList(oldAndNewReplicas), Sets.newHashSet(reassignedReplicas));
            } else {
                logger.info("Leader {} for partition %s being reassigned, is already in the new list of replicas %s but is dead", currentLeader, topicAndPartition,
                        reassignedReplicas);
                partitionStateMachine.handleStateChanges(Sets.newHashSet(topicAndPartition), PartitionState.OnlinePartition, reassignedPartitionLeaderSelector);
            }
        }
    }

    private void stopOldReplicasOfReassignedPartition(TopicAndPartition topicAndPartition,
                                                      ReassignedPartitionsContext reassignedPartitionContext,
                                                      Set<Integer> oldReplicas) {
        final String topic = topicAndPartition.topic;
        final int partition = topicAndPartition.partition;
        // first move the replica to offline state (the controller removes it from the ISR)
        Utils.foreach(oldReplicas, new Callable1<Integer>() {
            @Override
            public void apply(Integer replica) {
                replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topic, partition, replica)), ReplicaState.OfflineReplica);
            }
        });

        // send stop replica command to the old replicas
        Utils.foreach(oldReplicas, new Callable1<Integer>() {
            @Override
            public void apply(Integer replica) {
                replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topic, partition, replica)), ReplicaState.NonExistentReplica);
            }
        });
    }


    private void updateAssignedReplicasForPartition(final TopicAndPartition topicAndPartition,
                                                    List<Integer> replicas) {
        Multimap<TopicAndPartition, Integer> partitionsAndReplicasForThisTopic = Utils.filter(controllerContext.partitionReplicaAssignment, new Predicate2<TopicAndPartition, Collection<Integer>>() {
            @Override
            public boolean apply(TopicAndPartition _1, Collection<Integer> integers) {
                return _1.topic.equals(topicAndPartition.topic);
            }
        });
        partitionsAndReplicasForThisTopic.putAll(topicAndPartition, replicas);
        updateAssignedReplicasForPartition(topicAndPartition, partitionsAndReplicasForThisTopic);
        logger.info("Updated assigned replicas for partition {} being reassigned to {} ", topicAndPartition, replicas);
        // update the assigned replica list after a successful zookeeper write
        controllerContext.partitionReplicaAssignment.putAll(topicAndPartition, replicas);
    }

    private void startNewReplicasForReassignedPartition(final TopicAndPartition topicAndPartition,
                                                        ReassignedPartitionsContext reassignedPartitionContext,
                                                        Set<Integer> newReplicas) {
        // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
        // replicas list
        Utils.foreach(newReplicas, new Callable1<Integer>() {
            @Override
            public void apply(Integer replica) {
                replicaStateMachine.handleStateChanges(Sets.newHashSet(new PartitionAndReplica(topicAndPartition.topic, topicAndPartition.partition, replica)), ReplicaState.NewReplica);
            }
        });

    }

    private void updateLeaderEpochAndSendRequest(TopicAndPartition topicAndPartition, List<Integer> replicasToReceiveRequest, Set<Integer> newAssignedReplicas) {
        brokerRequestBatch.newBatch();
        LeaderIsrAndControllerEpoch updatedLeaderIsrAndControllerEpoch = updateLeaderEpoch(topicAndPartition.topic, topicAndPartition.partition);

        if (updatedLeaderIsrAndControllerEpoch != null) {
            brokerRequestBatch.addLeaderAndIsrRequestForBrokers(replicasToReceiveRequest, topicAndPartition.topic,
                    topicAndPartition.partition, updatedLeaderIsrAndControllerEpoch, newAssignedReplicas);
            brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch, controllerContext.correlationId.getAndIncrement());
            stateChangeLogger.trace("Controller {} epoch {} sent LeaderAndIsr request {} with new assigned replica list {} " +
                    "to leader {} for partition being reassigned {}", config.brokerId, controllerContext.epoch, updatedLeaderIsrAndControllerEpoch,
                    newAssignedReplicas, updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader, topicAndPartition);
        } else { // fail the reassignment
            stateChangeLogger.error("Controller {} epoch {} failed to send LeaderAndIsr request with new assigned replica list {} " +
                    "to leader for partition being reassigned {}", config.brokerId, controllerContext.epoch,
                    newAssignedReplicas, topicAndPartition);
        }
    }

    private void registerReassignedPartitionsListener() {
        zkClient.subscribeDataChanges(ZkUtils.ReassignPartitionsPath, new PartitionsReassignedListener(this));
    }

    private void registerPreferredReplicaElectionListener() {
        zkClient.subscribeDataChanges(ZkUtils.PreferredReplicaLeaderElectionPath, new PreferredReplicaElectionListener(this));
    }

    private void registerControllerChangedListener() {
        zkClient.subscribeDataChanges(ZkUtils.ControllerEpochPath, new ControllerEpochListener(this));
    }

    public void removePartitionFromReassignedPartitions(TopicAndPartition topicAndPartition) {
        if (controllerContext.partitionsBeingReassigned.get(topicAndPartition) != null) {
            // stop watching the ISR changes for this partition
            zkClient.unsubscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topicAndPartition.topic, topicAndPartition.partition),
                    controllerContext.partitionsBeingReassigned.get(topicAndPartition).isrChangeListener);
        }
        // read the current list of reassigned partitions from zookeeper
        Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
        // remove this partition from that list
        Map<TopicAndPartition, ReassignedPartitionsContext> updatedPartitionsBeingReassigned = Utils.minus(partitionsBeingReassigned, topicAndPartition);
        // write the new list to zookeeper
        ZkUtils.updatePartitionReassignmentData(zkClient, Utils.mapValues(updatedPartitionsBeingReassigned, new Function1<ReassignedPartitionsContext, Collection<Integer>>() {
            @Override
            public Collection<Integer> apply(ReassignedPartitionsContext _) {
                return _.newReplicas;
            }
        }));
        // update the cache. NO-OP if the partition's reassignment was never started
        controllerContext.partitionsBeingReassigned.remove(topicAndPartition);
    }

    public void updateAssignedReplicasForPartition(TopicAndPartition topicAndPartition,
                                                   Multimap<TopicAndPartition, Integer> newReplicaAssignmentForTopic) {
        try {
            String zkPath = ZkUtils.getTopicPath(topicAndPartition.topic);
            String jsonPartitionMap = ZkUtils.replicaAssignmentZkData(Utils.map(newReplicaAssignmentForTopic,new Function2<TopicAndPartition, Collection<Integer>, Tuple2<String, Collection<Integer>>>() {
                @Override
                public Tuple2<String, Collection<Integer>> apply(TopicAndPartition _1, Collection<Integer> _2) {
                    return Tuple2.make(_1.partition + "", _2);
                }
            }));
            ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionMap);
            logger.debug("Updated path {} with {} for replica assignment", zkPath, jsonPartitionMap);
        } catch (ZkNoNodeException e) {
            throw new IllegalStateException(String.format("Topic %s doesn't exist", topicAndPartition.topic));
        } catch (Throwable e) {
            throw new KafkaException(e.toString());
        }
    }

    public void removePartitionsFromPreferredReplicaElection(Set<TopicAndPartition> partitionsToBeRemoved) {
        for (TopicAndPartition partition : partitionsToBeRemoved) {
            // check the status
            int currentLeader = controllerContext.partitionLeadershipInfo.get(partition).leaderAndIsr.leader;
            Integer preferredReplica = Utils.head(controllerContext.partitionReplicaAssignment.get(partition));
            if (currentLeader == preferredReplica) {
                logger.info("Partition {} completed preferred replica leader election. New leader is {}", partition, preferredReplica);
            } else {
                logger.warn("Partition {} failed to complete preferred replica leader election. Leader is {}", partition, currentLeader);
            }
        }
        ZkUtils.deletePath(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath);
        controllerContext.partitionsUndergoingPreferredReplicaElection.removeAll(partitionsToBeRemoved);
    }


    private Set<PartitionAndReplica> getAllReplicasForPartition(Set<TopicAndPartition> partitions) {
        final Set<PartitionAndReplica> set = Sets.newHashSet();
        Utils.foreach(partitions, new Callable1<TopicAndPartition>() {
            @Override
            public void apply(final TopicAndPartition p) {
                Collection<Integer> replicas = controllerContext.partitionReplicaAssignment.get(p);
                Utils.foreach(replicas, new Callable1<Integer>() {
                    @Override
                    public void apply(Integer r) {
                        set.add(new PartitionAndReplica(p.topic, p.partition, r));
                    }
                });
            }
        });

        return set;
    }

    private void sendUpdateMetadataRequest(Set<Integer> brokers) {
        sendUpdateMetadataRequest(brokers, Collections.<TopicAndPartition>emptySet());
    }

    /**
     * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
     * metadata requests
     *
     * @param brokers    The brokers that the update metadata request should be sent to
     * @param partitions The partitions for which the metadata is to be sent
     */
    private void sendUpdateMetadataRequest(Set<Integer> brokers, Set<TopicAndPartition> partitions) {
        brokerRequestBatch.newBatch();
        brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions);
        brokerRequestBatch.sendRequestsToBrokers(epoch(), controllerContext.correlationId.getAndIncrement());
    }

    /**
     * Removes a given partition replica from the ISR; if it is not the current
     * leader and there are sufficient remaining replicas in ISR.
     *
     * @param topic     topic
     * @param partition partition
     * @param replicaId replica Id
     * @return the new leaderAndIsr (with the replica removed if it was present),
     * or None if leaderAndIsr is empty.
     */
    public LeaderIsrAndControllerEpoch removeReplicaFromIsr(String topic, int partition, final int replicaId) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        logger.debug("Removing replica {} from ISR {} for partition {}.", replicaId,
                controllerContext.partitionLeadershipInfo.get(topicAndPartition).leaderAndIsr.isr, topicAndPartition);
        LeaderIsrAndControllerEpoch finalLeaderIsrAndControllerEpoch = null;
        boolean zkWriteCompleteOrUnnecessary = false;
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);
            if (leaderIsrAndEpoch == null) {
                logger.warn("Cannot remove replica {} from ISR of {} - leaderAndIsr is empty.", replicaId, topicAndPartition);
                zkWriteCompleteOrUnnecessary = true;
            } else {
                // increment the leader epoch even if the ISR changes
                LeaderAndIsr leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
                int controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
                if (controllerEpoch > epoch())
                    throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                            "means the current controller with epoch %d went through a soft failure and another " +
                            "controller was elected with epoch %d. Aborting state change by this controller", epoch(), controllerEpoch);
                if (leaderAndIsr.isr.contains(replicaId)) {
                    // if the replica to be removed from the ISR is also the leader, set the new leader value to -1
                    int newLeader = (replicaId == leaderAndIsr.leader) ? -1 : leaderAndIsr.leader;
                    LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(newLeader, leaderAndIsr.leaderEpoch + 1,
                            Utils.filter(leaderAndIsr.isr, new Predicate<Integer>() {
                                @Override
                                public boolean apply(Integer b) {
                                    return b != replicaId;
                                }
                            }), leaderAndIsr.zkVersion + 1);
                    // update the new leadership decision in zookeeper or retry
                    Tuple2<Boolean, Integer> booleanIntegerTuple2 = ZkUtils.conditionalUpdatePersistentPath(
                            zkClient,
                            ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                            ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch()),
                            leaderAndIsr.zkVersion);
                    Boolean updateSucceeded = booleanIntegerTuple2._1;
                    Integer newVersion = booleanIntegerTuple2._2;
                    newLeaderAndIsr.zkVersion = newVersion;

                    finalLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch());
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch);
                    if (updateSucceeded)
                        logger.info("New leader and ISR for partition {} is {}", topicAndPartition, newLeaderAndIsr);
                    zkWriteCompleteOrUnnecessary = updateSucceeded;
                } else {
                    logger.warn("Cannot remove replica {} from ISR of partition {} since it is not in the ISR. Leader = {} ; ISR = {}",
                            replicaId, topicAndPartition, leaderAndIsr.leader, leaderAndIsr.isr);
                    finalLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(leaderAndIsr, epoch());
                    controllerContext.partitionLeadershipInfo.put(topicAndPartition, finalLeaderIsrAndControllerEpoch);
                    zkWriteCompleteOrUnnecessary = true;
                }
            }
        }

        return finalLeaderIsrAndControllerEpoch;
    }

    /**
     * Does not change leader or isr, but just increments the leader epoch
     *
     * @param topic     topic
     * @param partition partition
     * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
     */
    private LeaderIsrAndControllerEpoch updateLeaderEpoch(String topic, int partition) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        logger.debug("Updating leader epoch for partition {}.", topicAndPartition);
        LeaderIsrAndControllerEpoch finalLeaderIsrAndControllerEpoch = null;
        boolean zkWriteCompleteOrUnnecessary = false;
        while (!zkWriteCompleteOrUnnecessary) {
            // refresh leader and isr from zookeeper again
            LeaderIsrAndControllerEpoch leaderIsrAndEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition);

            if (leaderIsrAndEpoch == null) {
                throw new IllegalStateException(String.format("Cannot update leader epoch for partition %s as leaderAndIsr path is empty. " +
                        "This could mean we somehow tried to reassign a partition that doesn't exist", topicAndPartition));
            }

            LeaderAndIsr leaderAndIsr = leaderIsrAndEpoch.leaderAndIsr;
            int controllerEpoch = leaderIsrAndEpoch.controllerEpoch;
            if (controllerEpoch > epoch())
                throw new StateChangeFailedException("Leader and isr path written by another controller. This probably" +
                        "means the current controller with epoch %d went through a soft failure and another " +
                        "controller was elected with epoch %d. Aborting state change by this controller", epoch(), controllerEpoch);
            // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
            // assigned replica list
            LeaderAndIsr newLeaderAndIsr = new LeaderAndIsr(leaderAndIsr.leader, leaderAndIsr.leaderEpoch + 1,
                    leaderAndIsr.isr, leaderAndIsr.zkVersion + 1);
            // update the new leadership decision in zookeeper or retry
            Tuple2<Boolean, Integer> booleanIntegerTuple2 = ZkUtils.conditionalUpdatePersistentPath(
                    zkClient,
                    ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
                    ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, epoch()),
                    leaderAndIsr.zkVersion);
            Boolean updateSucceeded = booleanIntegerTuple2._1;
            Integer newVersion = booleanIntegerTuple2._2;
            newLeaderAndIsr.zkVersion = newVersion;
            finalLeaderIsrAndControllerEpoch = new LeaderIsrAndControllerEpoch(newLeaderAndIsr, epoch());
            if (updateSucceeded)
                logger.info("Updated leader epoch for partition {} to {}", topicAndPartition, newLeaderAndIsr.leaderEpoch);
            zkWriteCompleteOrUnnecessary = updateSucceeded;

        }

        return finalLeaderIsrAndControllerEpoch;
    }

    class SessionExpirationListener implements IZkStateListener {
        Logger logger = LoggerFactory.getLogger(SessionExpirationListener.class + "[SessionExpirationListener on " + config.brokerId + "], ");


        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
            // do nothing, since zkclient will do reconnect for us.
        }

        /**
         * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
         * any ephemeral nodes here.
         */
        @Override
        public void handleNewSession() throws Exception {
            logger.info("ZK expired; shut down all controller components and try to re-elect");
            synchronized (controllerContext.controllerLock) {
                Utils.unregisterMBean(KafkaControllers.MBeanName);
                partitionStateMachine.shutdown();
                replicaStateMachine.shutdown();
                if (controllerContext.controllerChannelManager != null) {
                    controllerContext.controllerChannelManager.shutdown();
                    controllerContext.controllerChannelManager = null;
                }
                controllerElector.elect();
            }
        }

        @Override
        public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

        }
    }

    private void checkAndTriggerPartitionRebalance() {
        if (!isActive()) return;

        logger.trace("checking need to trigger partition rebalance");
        // get all the active brokers
        Table<Integer, TopicAndPartition, Collection<Integer>> preferredReplicasForTopicsByBrokers = null;
        synchronized (controllerContext.controllerLock) {
            preferredReplicasForTopicsByBrokers = Utils.groupBy(controllerContext.partitionReplicaAssignment,
                    new Function2<TopicAndPartition, Collection<Integer>, Integer>() {
                        @Override
                        public Integer apply(TopicAndPartition topicAndPartition, Collection<Integer> assignedReplicas) {
                            return Utils.head(assignedReplicas);
                        }
                    });

        }
        logger.debug("preferred replicas by broker {}", preferredReplicasForTopicsByBrokers);
        // for each broker, check if a preferred replica election needs to be triggered
        Utils.foreach(preferredReplicasForTopicsByBrokers, new Callable2<Integer, Map<TopicAndPartition, Collection<Integer>>>() {
            @Override
            public void apply(final Integer leaderBroker, Map<TopicAndPartition, Collection<Integer>> topicAndPartitionsForBroker) {
                double imbalanceRatio = 0;
                Multimap<TopicAndPartition, Integer> topicsNotInPreferredReplica = null;
                synchronized (controllerContext.controllerLock) {
                    topicsNotInPreferredReplica = Utils.filter(Utils.multimap(topicAndPartitionsForBroker), new Predicate2<TopicAndPartition, Collection<Integer>>() {
                        @Override
                        public boolean apply(TopicAndPartition topicPartition, Collection<Integer> replicas) {
                            return controllerContext.partitionLeadershipInfo.get(topicPartition).leaderAndIsr.leader != leaderBroker;
                        }
                    });

                    logger.debug("topics not in preferred replica {}", topicsNotInPreferredReplica);
                    int totalTopicPartitionsForBroker = topicAndPartitionsForBroker.size();
                    int totalTopicPartitionsNotLedByBroker = topicsNotInPreferredReplica.size();
                    imbalanceRatio = ((double) totalTopicPartitionsNotLedByBroker) / totalTopicPartitionsForBroker;
                    logger.trace("leader imbalance ratio for broker {} is {}", leaderBroker, imbalanceRatio);
                }
                // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
                // that need to be on this broker
                if (imbalanceRatio > (((double) config.leaderImbalancePerBrokerPercentage) / 100)) {
                    synchronized (controllerContext.controllerLock) {
                        // do this check only if the broker is live and there are no partitions being reassigned currently
                        // and preferred replica election is not in progress
                        if (controllerContext.liveBrokerIds().contains(leaderBroker) &&
                                controllerContext.partitionsBeingReassigned.size() == 0 &&
                                controllerContext.partitionsUndergoingPreferredReplicaElection.size() == 0) {
                            String zkPath = ZkUtils.PreferredReplicaLeaderElectionPath;
                            final List<Map<String, Object>> partitionsList = Lists.newArrayList();
                            Utils.foreach(topicsNotInPreferredReplica.keySet(), new Callable1<TopicAndPartition>() {
                                @Override
                                public void apply(TopicAndPartition e) {
                                    Map<String, Object> map = Maps.newHashMap();
                                    partitionsList.add(map);

                                    map.put("topic", e.topic);
                                    map.put("partition", e.partition);
                                }
                            });

                            Map<String, Object> jsonMap = Maps.newHashMap();
                            jsonMap.put("version", 1);
                            jsonMap.put("partitions", partitionsList);

                            String jsonData = Json.encode(jsonMap);
                            try {
                                ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
                                logger.info("Created preferred replica election path with {}", jsonData);
                            } catch (ZkNodeExistsException e) {
                                Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection =
                                        PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(ZkUtils.readData(zkClient, zkPath)._1);
                                logger.error("Preferred replica leader election currently in progress for " +
                                        "{}. Aborting operation", partitionsUndergoingPreferredReplicaElection);
                            } catch (Throwable e) {
                                logger.error("Error while trying to auto rebalance topics {}", topicsNotInPreferredReplica.keySet());
                            }
                        }
                    }
                }
            }
        });
    }

    /**
     * Starts the partition reassignment process unless -
     * 1. Partition previously existed
     * 2. New replicas are the same as existing replicas
     * 3. Any replica in the new set of replicas are dead
     * If any of the above conditions are satisfied, it logs an error and removes the partition from list of reassigned
     * partitions.
     */
    class PartitionsReassignedListener implements IZkDataListener {
        public KafkaController controller;

        public ZkClient zkClient;
        public ControllerContext controllerContext;

        Logger logger;

        PartitionsReassignedListener(KafkaController controller) {
            this.controller = controller;

            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
            logger = LoggerFactory.getLogger(PartitionsReassignedListener.class + "[PartitionsReassignedListener on " + controller.config.brokerId + "]: ");
        }

        /**
         * Invoked when some partitions are reassigned by the admin command
         */
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            logger.debug("Partitions reassigned listener fired for path {}. Record partitions to be reassigned {}",
                    dataPath, data);
            Multimap<TopicAndPartition, Integer> partitionsReassignmentData = ZkUtils.parsePartitionReassignmentData(data.toString());
            Multimap<TopicAndPartition, Integer> newPartitions = Utils.filter(partitionsReassignmentData, new Predicate2<TopicAndPartition, Collection<Integer>>() {
                @Override
                public boolean apply(TopicAndPartition _1, Collection<Integer> integers) {
                    return !controllerContext.partitionsBeingReassigned.containsKey(_1);
                }
            });
            Utils.foreach(newPartitions, new Callable2<TopicAndPartition, Collection<Integer>>() {
                @Override
                public void apply(TopicAndPartition topicAndPartition, Collection<Integer> newReplicas) {
                    synchronized (controllerContext.controllerLock) {
                        ReassignedPartitionsContext context = new ReassignedPartitionsContext(newReplicas);
                        controller.initiateReassignReplicasForTopicPartition(topicAndPartition, context);
                    }
                }
            });
        }

        /**
         * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
         */
        @Override
        public void handleDataDeleted(String s) throws Exception {

        }
    }

    public static class ReassignedPartitionsIsrChangeListener implements IZkDataListener {
        public KafkaController controller;
        public String topic;
        public int partition;
        public Set<Integer> reassignedReplicas;

        ReassignedPartitionsIsrChangeListener(KafkaController controller,
                                              String topic,
                                              int partition,
                                              Set<Integer> reassignedReplicas) {
            this.controller = controller;
            this.topic = topic;
            this.partition = partition;
            this.reassignedReplicas = reassignedReplicas;

            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
            logger = LoggerFactory.getLogger(ReassignedPartitionsIsrChangeListener.class + "[ReassignedPartitionsIsrChangeListener on controller " + controller.config.brokerId + "]: ");
        }

        Logger logger;
        public ZkClient zkClient;
        public ControllerContext controllerContext;

        // Invoked when some partitions need to move leader to preferred replica
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            try {
                synchronized (controllerContext.controllerLock) {
                    logger.debug("Reassigned partitions isr change listener fired for path %s with children {}", dataPath, data);
                    // check if this partition is still being reassigned or not
                    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
                    ReassignedPartitionsContext reassignedPartitionContext = controllerContext.partitionsBeingReassigned.get(topicAndPartition);
                    if (reassignedPartitionContext != null) {
                        // need to re-read leader and isr from zookeeper since the zkclient callback doesn't return the Stat object
                        LeaderAndIsr leaderAndIsr = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition);
                        if (leaderAndIsr != null) { // check if new replicas have joined ISR
                            Set<Integer> caughtUpReplicas = Utils.and(reassignedReplicas, leaderAndIsr.isr);
                            if (caughtUpReplicas.equals(reassignedReplicas)) {
                                // resume the partition reassignment process
                                logger.info("{}/{} replicas have caught up with the leader for partition {} being reassigned.Resuming partition reassignment",
                                        caughtUpReplicas.size(), reassignedReplicas.size(), topicAndPartition);
                                controller.onPartitionReassignment(topicAndPartition, reassignedPartitionContext);
                            } else {
                                logger.info("{}/{} replicas have caught up with the leader for partition {} being reassigned. Replica(s) {} still need to catch up",
                                        caughtUpReplicas.size(), reassignedReplicas.size(), topicAndPartition, Utils.minus(reassignedReplicas, leaderAndIsr.isr));
                            }
                        } else {
                            logger.error("Error handling reassignment of partition {} to replicas {} as it was never created",
                                    topicAndPartition, reassignedReplicas);
                        }
                    }
                }
            } catch (Throwable e) {
                logger.error("Error while handling partition reassignment", e);
            }
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {

        }
    }

    /**
     * Starts the preferred replica leader election for the list of partitions specified under
     * /admin/preferred_replica_election -
     */
    public static class PreferredReplicaElectionListener implements IZkDataListener {
        public KafkaController controller;

        public PreferredReplicaElectionListener(KafkaController controller) {
            this.controller = controller;

            zkClient = controller.controllerContext.zkClient;
            controllerContext = controller.controllerContext;
            logger = LoggerFactory.getLogger(PreferredReplicaElectionListener.class + "[PreferredReplicaElectionListener on " + controller.config.brokerId + "]: ");

        }

        Logger logger;
        public ZkClient zkClient;
        public ControllerContext controllerContext;

        // Invoked when some partitions are reassigned by the admin command
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            logger.debug("Preferred replica election listener fired for path {}. Record partitions to undergo preferred replica election {}",
                    dataPath, data.toString());
            Set<TopicAndPartition> partitionsForPreferredReplicaElection = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(data.toString());

            synchronized (controllerContext.controllerLock) {
                logger.info("These partitions are already undergoing preferred replica election: {}",
                        controllerContext.partitionsUndergoingPreferredReplicaElection);
                Set<TopicAndPartition> newPartitions = Utils.minus(partitionsForPreferredReplicaElection, controllerContext.partitionsUndergoingPreferredReplicaElection);
                controller.onPreferredReplicaElection(newPartitions);
            }
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {

        }
    }

    public static class ControllerEpochListener implements IZkDataListener {
        public KafkaController controller;

        public ControllerEpochListener(KafkaController controller) {
            this.controller = controller;

            controllerContext = controller.controllerContext;
            logger = LoggerFactory.getLogger(ControllerEpochListener.class + "[ControllerEpochListener on " + controller.config.brokerId + "]: ");

            readControllerEpochFromZookeeper();
        }

        Logger logger;
        public ControllerContext controllerContext;


        // Invoked when a controller updates the epoch value
        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            logger.debug("Controller epoch listener fired with new epoch {}", data.toString());
            synchronized (controllerContext.controllerLock) {
                // read the epoch path to get the zk version
                readControllerEpochFromZookeeper();
            }
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {

        }

        private void readControllerEpochFromZookeeper() {
            // initialize the controller epoch and zk version by reading from zookeeper
            if (ZkUtils.pathExists(controllerContext.zkClient, ZkUtils.ControllerEpochPath)) {
                Tuple2<String, Stat> epochData = ZkUtils.readData(controllerContext.zkClient, ZkUtils.ControllerEpochPath);
                controllerContext.epoch = Integer.parseInt(epochData._1);
                controllerContext.epochZkVersion = epochData._2.getVersion();
                logger.info("Initialized controller epoch to {} and zk version {}", controllerContext.epoch, controllerContext.epochZkVersion);
            }
        }
    }
}

