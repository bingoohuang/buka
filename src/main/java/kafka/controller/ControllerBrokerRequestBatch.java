package kafka.controller;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ControllerBrokerRequestBatch {
    public ControllerContext controllerContext;
    public Callable3<Integer, RequestOrResponse, Callable1<RequestOrResponse>> sendRequest;
    public int controllerId;
    public String clientId;

    public ControllerBrokerRequestBatch(ControllerContext controllerContext,
                                        Callable3<Integer, RequestOrResponse, Callable1<RequestOrResponse>> sendRequest,
                                        int controllerId,
                                        String clientId) {
        this.controllerContext = controllerContext;
        this.sendRequest = sendRequest;
        this.controllerId = controllerId;
        this.clientId = clientId;
    }

    public Table<Integer, Tuple2<String, Integer>, PartitionStateInfo> leaderAndIsrRequestMap = HashBasedTable.create();
    public Multimap<Integer, Tuple2<String, Integer>> stopReplicaRequestMap = HashMultimap.create();
    public Multimap<Integer, Tuple2<String, Integer>> stopAndDeleteReplicaRequestMap = HashMultimap.create();
    public Table<Integer, TopicAndPartition, PartitionStateInfo> updateMetadataRequestMap = HashBasedTable.create();
    private Logger stateChangeLogger = LoggerFactory.getLogger(KafkaControllers.stateChangeLogger);

    Logger logger = LoggerFactory.getLogger(ControllerBrokerRequestBatch.class);

    public void newBatch() {
        // raise error if the previous batch is not empty
        if (leaderAndIsrRequestMap.size() > 0)
            throw new IllegalStateException(String.format("Controller to broker state change requests batch is not empty while creating " +
                    "a new one. Some LeaderAndIsr state changes %s might be lost ", leaderAndIsrRequestMap.toString()));
        if (stopReplicaRequestMap.size() > 0)
            throw new IllegalStateException(String.format("Controller to broker state change requests batch is not empty while creating a " +
                    "new one. Some StopReplica state changes %s might be lost ", stopReplicaRequestMap.toString()));
        if (updateMetadataRequestMap.size() > 0)
            throw new IllegalStateException(String.format("Controller to broker state change requests batch is not empty while creating a " +
                    "new one. Some UpdateMetadata state changes %s might be lost ", updateMetadataRequestMap.toString()));
        if (stopAndDeleteReplicaRequestMap.size() > 0)
            throw new IllegalStateException(String.format("Controller to broker state change requests batch is not empty while creating a " +
                    "new one. Some StopReplica with delete state changes %s might be lost ", stopAndDeleteReplicaRequestMap.toString()));
        leaderAndIsrRequestMap.clear();
        stopReplicaRequestMap.clear();
        updateMetadataRequestMap.clear();
        stopAndDeleteReplicaRequestMap.clear();
    }

    public void addLeaderAndIsrRequestForBrokers(List<Integer> brokerIds, final String topic, final int partition,
                                                 final LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch,
                                                 final Set<Integer> replicas) {
        Utils.foreach(brokerIds, new Callable1<Integer>() {
            @Override
            public void apply(Integer brokerId) {
                leaderAndIsrRequestMap.put(brokerId, Tuple2.make(topic, partition),
                        new PartitionStateInfo(leaderIsrAndControllerEpoch, replicas));

            }
        });

        addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds(),
                Sets.newHashSet(new TopicAndPartition(topic, partition)));
    }

    public void addStopReplicaRequestForBrokers(List<Integer> brokerIds, final String topic,
                                                final int partition, final boolean deletePartition) {
        Utils.foreach(brokerIds, new Callable1<Integer>() {
            @Override
            public void apply(Integer brokerId) {
                if (deletePartition) {
                    stopAndDeleteReplicaRequestMap.put(brokerId, Tuple2.make(topic, partition));
                } else {
                    stopReplicaRequestMap.put(brokerId, Tuple2.make(topic, partition));
                }
            }
        });
    }

    public void addUpdateMetadataRequestForBrokers(final Set<Integer> brokerIds, Set<TopicAndPartition> partitions) {
        Set<TopicAndPartition> partitionList = partitions.isEmpty()
                ? controllerContext.partitionLeadershipInfo.keySet()
                : partitions;

        Utils.foreach(partitionList, new Callable1<TopicAndPartition>() {
            @Override
            public void apply(final TopicAndPartition partition) {
                LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch =
                        controllerContext.partitionLeadershipInfo.get(partition);
                if (leaderIsrAndControllerEpoch != null) {
                    Set<Integer> replicas = Sets.newHashSet(controllerContext.partitionReplicaAssignment.get(partition));
                    final PartitionStateInfo partitionStateInfo = new PartitionStateInfo(leaderIsrAndControllerEpoch, replicas);

                    Utils.foreach(brokerIds, new Callable1<Integer>() {
                        @Override
                        public void apply(Integer brokerId) {
                            updateMetadataRequestMap.put(brokerId, partition, partitionStateInfo);
                        }
                    });
                } else {
                    logger.info("Leader not assigned yet for partition {}. Skip sending udpate metadata request", partition);
                }
            }
        });
    }

    public void sendRequestsToBrokers(final int controllerEpoch, final int correlationId) {
        Utils.foreach(leaderAndIsrRequestMap, new Callable2<Integer, Map<Tuple2<String, Integer>, PartitionStateInfo>>() {
            @Override
            public void apply(final Integer broker, Map<Tuple2<String, Integer>, PartitionStateInfo> partitionStateInfos) {
                final Set<Integer> leaderIds = Utils.mapSet(partitionStateInfos.values(),
                        new Function1<PartitionStateInfo, Integer>() {
                            @Override
                            public Integer apply(PartitionStateInfo arg) {
                                return arg.leaderIsrAndControllerEpoch.leaderAndIsr.leader;
                            }
                        });

                List<Broker> leaders = Utils.filter(controllerContext.liveOrShuttingDownBrokers(),
                        new Predicate<Broker>() {
                            @Override
                            public boolean apply(Broker b) {
                                return leaderIds.contains(b.id);
                            }
                        });

                LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest(partitionStateInfos,
                        Sets.newHashSet(leaders),
                        controllerId, controllerEpoch, correlationId, clientId);

                Utils.foreach(partitionStateInfos, new Callable2<Tuple2<String, Integer>, PartitionStateInfo>() {
                    @Override
                    public void apply(Tuple2<String, Integer> _1, PartitionStateInfo _2) {
                        String typeOfRequest = (broker == _2.leaderIsrAndControllerEpoch.leaderAndIsr.leader)
                                ? "become-leader" : "become-follower";
                        stateChangeLogger.trace("Controller {} epoch {} sending {} LeaderAndIsr request " +
                                "{} with correlationId {} to broker {} " +
                                "for partition [{},{}]", controllerId, controllerEpoch, typeOfRequest,
                                _2.leaderIsrAndControllerEpoch, correlationId, broker,
                                _1._1, _1._2);
                    }
                });

                sendRequest.apply(broker, leaderAndIsrRequest, null);
            }
        });


        leaderAndIsrRequestMap.clear();
        Utils.foreach(updateMetadataRequestMap, new Callable2<Integer, Map<TopicAndPartition, PartitionStateInfo>>() {
            @Override
            public void apply(final Integer broker, Map<TopicAndPartition, PartitionStateInfo> partitionStateInfos) {
                UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest(controllerId,
                        controllerEpoch, correlationId, clientId,
                        partitionStateInfos, controllerContext.liveOrShuttingDownBrokers());
                Utils.foreach(partitionStateInfos, new Callable2<TopicAndPartition, PartitionStateInfo>() {
                    @Override
                    public void apply(TopicAndPartition _1, PartitionStateInfo _2) {
                        stateChangeLogger.trace("Controller {} epoch {} sending UpdateMetadata request {} with " +
                                "correlationId {} to broker {} for partition {}", controllerId, controllerEpoch,
                                _2.leaderIsrAndControllerEpoch,
                                correlationId, broker, _1);
                    }
                });

                sendRequest.apply(broker, updateMetadataRequest, null);
            }
        });

        updateMetadataRequestMap.clear();


        Utils.foreach(Lists.newArrayList(Tuple2.make(stopReplicaRequestMap, false),
                Tuple2.make(stopAndDeleteReplicaRequestMap, true)),
                new Callable1<Tuple2<Multimap<Integer, Tuple2<String, Integer>>, Boolean>>() {
                    @Override
                    public void apply(final Tuple2<Multimap<Integer, Tuple2<String, Integer>>, Boolean> _) {
                        Multimap<Integer, Tuple2<String, Integer>> m = _._1;
                        final boolean deletePartitions = _._2;
                        Utils.foreach(m, new Callable2<Integer, Collection<Tuple2<String, Integer>>>() {

                            @Override
                            public void apply(Integer broker, Collection<Tuple2<String, Integer>> replicas) {
                                if (replicas.size() > 0) {
                                    logger.debug("The stop replica request (delete = {}) sent to broker {} is {}",
                                            deletePartitions, broker, replicas);
                                    StopReplicaRequest stopReplicaRequest = new StopReplicaRequest(deletePartitions,
                                            Sets.newHashSet(replicas), controllerId,
                                            controllerEpoch, correlationId);
                                    sendRequest.apply(broker, stopReplicaRequest, null);
                                }
                            }
                        });
                        m.clear();

                    }
                });
    }
}
