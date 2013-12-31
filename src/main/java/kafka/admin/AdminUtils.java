package kafka.admin;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.cluster.LogConfigs;
import kafka.common.*;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class AdminUtils {
    public static Random rand = new Random();
    public static String TopicConfigChangeZnodePrefix = "config_change_";
    static Logger logger = LoggerFactory.getLogger(AdminUtils.class);

    /**
     * There are 2 goals of replica assignment:
     * 1. Spread the replicas evenly among brokers.
     * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
     * <p/>
     * To achieve this goal, we:
     * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
     * 2. Assign the remaining replicas of each partition with an increasing shift.
     * <p/>
     * Here is an example of assigning
     * broker-0  broker-1  broker-2  broker-3  broker-4
     * p0        p1        p2        p3        p4       (1st replica)
     * p5        p6        p7        p8        p9       (1st replica)
     * p4        p0        p1        p2        p3       (2nd replica)
     * p8        p9        p5        p6        p7       (2nd replica)
     * p3        p4        p0        p1        p2       (3nd replica)
     * p7        p8        p9        p5        p6       (3nd replica)
     */
    public static Multimap<Integer, Integer> assignReplicasToBrokers(List<Integer> brokerList,
                                                                     int nPartitions,
                                                                     int replicationFactor) {
        return assignReplicasToBrokers(brokerList, nPartitions, replicationFactor, -1, -1);
    }

    public static Multimap<Integer, Integer> assignReplicasToBrokers(List<Integer> brokerList,
                                                                     int nPartitions,
                                                                     int replicationFactor,
                                                                     int fixedStartIndex/* = -1*/,
                                                                     int startPartitionId /*= -1*/) {
        if (nPartitions <= 0)
            throw new AdminOperationException("number of partitions must be larger than 0");
        if (replicationFactor <= 0)
            throw new AdminOperationException("replication factor must be larger than 0");
        if (replicationFactor > brokerList.size())
            throw new AdminOperationException("replication factor: " + replicationFactor +
                    " larger than available brokers: " + brokerList.size());
        Multimap<Integer, Integer> ret = HashMultimap.create();
        int startIndex = (fixedStartIndex >= 0) ? fixedStartIndex : rand.nextInt(brokerList.size());
        int currentPartitionId = (startPartitionId >= 0) ? startPartitionId : 0;

        int nextReplicaShift = (fixedStartIndex >= 0) ? fixedStartIndex : rand.nextInt(brokerList.size());
        for (int i = 0; i < nPartitions; ++i) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0))
                nextReplicaShift += 1;
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List<Integer> replicaList = Lists.newArrayList(brokerList.get(firstReplicaIndex));
            for (int j = 0; j < replicationFactor - 1; ++j)
                replicaList.add(brokerList.get(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size())));
            Collections.reverse(replicaList);
            ret.putAll(currentPartitionId, replicaList);
            currentPartitionId = currentPartitionId + 1;
        }
        return ret;
    }

    public static void addPartitions(ZkClient zkClient, String topic, int numPartitions /* = 1*/, String replicaAssignmentStr/* = ""*/) {
        Multimap<TopicAndPartition, Integer> existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(topic));
        if (existingPartitionsReplicaList.size() == 0)
            throw new AdminOperationException("The topic %s does not exist", topic);

        final Collection<Integer> existingReplicaList = Utils.head(existingPartitionsReplicaList)._2;
        int partitionsToAdd = numPartitions - existingPartitionsReplicaList.size();
        if (partitionsToAdd <= 0)
            throw new AdminOperationException("The number of partitions for a topic can only be increased");

        // create the new partition replication list
        List<Integer> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        Multimap<Integer, Integer> newPartitionReplicaList = (replicaAssignmentStr == null || replicaAssignmentStr == "") ?
                assignReplicasToBrokers(brokerList, partitionsToAdd, existingReplicaList.size(), Utils.head(existingReplicaList), existingPartitionsReplicaList.size())
                :
                getManualReplicaAssignment(replicaAssignmentStr, Sets.newHashSet(brokerList), existingPartitionsReplicaList.size());

        // check if manual assignment has the right replication factor
        Multimap<Integer, Integer> unmatchedRepFactorList = Utils.filter(newPartitionReplicaList, new Predicate2<Integer, Collection<Integer>>() {
            @Override
            public boolean apply(Integer integer, Collection<Integer> p) {
                return p.size() != existingReplicaList.size();
            }
        });
        if (unmatchedRepFactorList.size() != 0)
            throw new AdminOperationException("The replication factor in manual replication assignment " +
                    " is not equal to the existing replication factor for the topic " + existingReplicaList.size());

        logger.info("Add partition list for {} is {}", topic, newPartitionReplicaList);

        Multimap<Integer, Integer> partitionReplicaList = Utils.map(existingPartitionsReplicaList, new Function2<TopicAndPartition, Collection<Integer>, Tuple2<Integer, Collection<Integer>>>() {
            @Override
            public Tuple2<Integer, Collection<Integer>> apply(TopicAndPartition _1, Collection<Integer> _2) {
                return Tuple2.make(_1.partition, _2);
            }
        });
        // add the new list
        partitionReplicaList.putAll(newPartitionReplicaList);
        createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, new Properties(), /*update = */true);
    }

    public static Multimap<Integer, Integer> getManualReplicaAssignment(String replicaAssignmentList, Set<Integer> availableBrokerList, int startPartitionId) {
        String[] partitionList = replicaAssignmentList.split(",");
        Multimap<Integer, Integer> ret = HashMultimap.create();
        int partitionId = startPartitionId;
        partitionList = Utils.takeRight(partitionList, (partitionList.length - partitionId));
        for (int i = 0; i < partitionList.length; ++i) {
            List<Integer> brokerList = Utils.mapList(partitionList[i].split(":"), new Function1<String, Integer>() {
                @Override
                public Integer apply(String s) {
                    return Integer.parseInt(s.trim());
                }
            });
            if (brokerList.size() <= 0)
                throw new AdminOperationException("replication factor must be larger than 0");
            if (brokerList.size() != Sets.newHashSet(brokerList).size())
                throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList);
            if (!Utils.subsetOf(Sets.newHashSet(brokerList), availableBrokerList))
                throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList +
                        "available broker:" + availableBrokerList);
            ret.putAll(partitionId, brokerList);
            if (ret.get(partitionId).size() != ret.get(startPartitionId).size())
                throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList);
            partitionId = partitionId + 1;
        }

        return ret;
    }

    public static void deleteTopic(ZkClient zkClient, String topic) {
        zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
        zkClient.deleteRecursive(ZkUtils.getTopicConfigPath(topic));
    }

    public static Boolean topicExists(ZkClient zkClient, String topic) {
        return zkClient.exists(ZkUtils.getTopicPath(topic));
    }

    public static void createTopic(ZkClient zkClient,
                                   String topic,
                                   int partitions,
                                   int replicationFactor,
                                   Properties topicConfig  /*= new Properties*/) {
        List<Integer> brokerList = ZkUtils.getSortedBrokerList(zkClient);
        Multimap<Integer, Integer> replicaAssignment = assignReplicasToBrokers(brokerList, partitions, replicationFactor);
        createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig, false);
    }

    public static void createOrUpdateTopicPartitionAssignmentPathInZK(ZkClient zkClient,
                                                                      String topic,
                                                                      final Multimap<Integer, Integer> partitionReplicaAssignment,
                                                                      Properties config /*= new Properties*/,
                                                                      boolean update/* = false*/) {
        // validate arguments
        Topic.validate(topic);
        LogConfigs.validate(config);
        checkState(Utils.mapSet(partitionReplicaAssignment, new Function2<Integer, Collection<Integer>, Integer>() {
            @Override
            public Integer apply(Integer arg1, Collection<Integer> _) {
                return _.size();
            }
        }).size() == 1, "All partitions should have the same number of replicas.");

        String topicPath = ZkUtils.getTopicPath(topic);
        if (!update && zkClient.exists(topicPath))
            throw new TopicExistsException("Topic \"%s\" already exists.", topic);

        Utils.foreach(partitionReplicaAssignment, new Callable2<Integer, Collection<Integer>>() {
            @Override
            public void apply(Integer integer, Collection<Integer> reps) {
                checkState(reps.size() == Sets.newHashSet(reps).size(), "Duplicate replica assignment found: " + partitionReplicaAssignment);
            }
        });

        // write out the config if there is any, this isn't transactional with the partition assignments
        writeTopicConfig(zkClient, topic, config);

        // create the partition assignment
        writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update);
    }

    private static void writeTopicPartitionAssignment(ZkClient zkClient, String topic, Multimap<Integer, Integer> replicaAssignment, boolean update) {
        try {
            String zkPath = ZkUtils.getTopicPath(topic);
            String jsonPartitionData = ZkUtils.replicaAssignmentZkData(Utils.map(replicaAssignment, new Function2<Integer, Collection<Integer>, Tuple2<String, Collection<Integer>>>() {
                @Override
                public Tuple2<String, Collection<Integer>> apply(Integer _1, Collection<Integer> _2) {
                    return Tuple2.make(_1 + "", _2);
                }
            }));

            if (!update) {
                logger.info("Topic creation {}", jsonPartitionData);
                ZkUtils.createPersistentPath(zkClient, zkPath, jsonPartitionData);
            } else {
                logger.info("Topic update {}", jsonPartitionData);
                ZkUtils.updatePersistentPath(zkClient, zkPath, jsonPartitionData);
            }
            logger.debug("Updated path {} with {} for replica assignment", zkPath, jsonPartitionData);
        } catch (ZkNodeExistsException e) {
            throw new TopicExistsException("topic %s already exists", topic);
        } catch (Throwable e) {
            throw new AdminOperationException(e.toString());
        }
    }

    /**
     * Update the config for an existing topic and create a change notification so the change will propagate to other brokers
     *
     * @param zkClient: The ZkClient handle used to write the new config to zookeeper
     * @param topic:    The topic for which configs are being changed
     * @param configs:  The final set of configs that will be applied to the topic. If any new configs need to be added or
     *                  existing configs need to be deleted, it should be done prior to invoking this API
     */
    public static void changeTopicConfig(ZkClient zkClient, String topic, Properties configs) {
        if (!topicExists(zkClient, topic))
            throw new AdminOperationException("Topic \"%s\" does not exist.", (topic));

        // remove the topic overrides
        LogConfigs.validate(configs);

        // write the new config--may not exist if there were previously no overrides
        writeTopicConfig(zkClient, topic, configs);

        // create the change notification
        zkClient.createPersistentSequential(ZkUtils.TopicConfigChangesPath + "/" + TopicConfigChangeZnodePrefix, Json.encode(topic));
    }

    /**
     * Write out the topic config to zk, if there is any
     */
    private static void writeTopicConfig(ZkClient zkClient, String topic, Properties config) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("version", 1);
        map.put("config", config);

        ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicConfigPath(topic), Json.encode(map));
    }

    /**
     * Read the topic config (if any) from zk
     */
    public static Properties fetchTopicConfig(ZkClient zkClient, String topic) {
        String str = zkClient.readData(ZkUtils.getTopicConfigPath(topic), true);
        Properties props = new Properties();
        if (str != null) {
            JSONObject map = Json.parseFull(str);
            if (map == null) {
                // there are no config overrides
            } else {
                checkState(map.get("version") == 1);
                JSONObject config = map.getJSONObject("config");
                if (config == null) {
                    throw new IllegalArgumentException("Invalid topic config: " + str);
                }

                props.putAll(config);

                // case o => throw new IllegalArgumentException("Unexpected value in config: "  + str)
            }
        }
        return props;
    }

    public static Map<String, Properties> fetchAllTopicConfigs(final ZkClient zkClient) {
        return Utils.map(ZkUtils.getAllTopics(zkClient), new Function1<String, Tuple2<String, Properties>>() {
            @Override
            public Tuple2<String, Properties> apply(String topic) {
                return Tuple2.make(topic, fetchTopicConfig(zkClient, topic));
            }
        });
    }

    public static TopicMetadata fetchTopicMetadataFromZk(String topic, ZkClient zkClient) {
        return fetchTopicMetadataFromZk(topic, zkClient, Maps.<Integer, Broker>newHashMap());
    }

    public static Set<TopicMetadata> fetchTopicMetadataFromZk(Set<String> topics, final ZkClient zkClient) {
        final Map<Integer, Broker> cachedBrokerInfo = Maps.newHashMap();

        return Utils.mapSet(topics, new Function1<String, TopicMetadata>() {
            @Override
            public TopicMetadata apply(String topic) {
                return fetchTopicMetadataFromZk(topic, zkClient, cachedBrokerInfo);
            }
        });
    }

    private static TopicMetadata fetchTopicMetadataFromZk(final String topic, final ZkClient zkClient, final Map<Integer, Broker> cachedBrokerInfo) {
        if (ZkUtils.pathExists(zkClient, ZkUtils.getTopicPath(topic))) {
            Multimap<Integer, Integer> topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, Lists.newArrayList(topic)).get(topic);
            List<Tuple2<Integer, Collection<Integer>>> sortedPartitions = Utils.sortWith(Utils.toList(topicPartitionAssignment), new Comparator<Tuple2<Integer, Collection<Integer>>>() {
                @Override
                public int compare(Tuple2<Integer, Collection<Integer>> m1, Tuple2<Integer, Collection<Integer>> m2) {
                    return m1._1 < m2._1 ? -1 : (m1._1.equals(m2._1) ? 0 : -1);
                }
            });
            List<PartitionMetadata> partitionMetadata = Utils.mapList(sortedPartitions, new Function1<Tuple2<Integer, Collection<Integer>>, PartitionMetadata>() {
                @Override
                public PartitionMetadata apply(Tuple2<Integer, Collection<Integer>> partitionMap) {
                    int partition = partitionMap._1;
                    Collection<Integer> replicas = partitionMap._2;
                    List<Integer> inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partition);
                    Integer leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition);
                    logger.debug("replicas = " + replicas + ", in sync replicas = " + inSyncReplicas + ", leader = " + leader);

                    Broker leaderInfo = null;
                    List<Broker> replicaInfo = null;
                    List<Broker> isrInfo = null;
                    try {
                        if (leader == null) {
                            throw new LeaderNotAvailableException("No leader exists for partition " + partition);
                        }

                        try {
                            leaderInfo = Utils.head(getBrokerInfoFromCache(zkClient, cachedBrokerInfo, Lists.newArrayList(leader)));
                        } catch (Throwable e) {
                            throw new LeaderNotAvailableException(e, "Leader not available for partition [%s,%d]", topic, partition);
                        }

                        try {
                            replicaInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, replicas);
                            isrInfo = getBrokerInfoFromCache(zkClient, cachedBrokerInfo, inSyncReplicas);
                        } catch (Throwable e) {
                            throw new ReplicaNotAvailableException(e);
                        }
                        if (replicaInfo.size() < replicas.size()) {
                            final List<Integer> brokerIds = Utils.mapList(replicaInfo, new Function1<Broker, Integer>() {
                                @Override
                                public Integer apply(Broker _) {
                                    return _.id;
                                }
                            });
                            throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                                    Utils.filter(replicas, new Predicate<Integer>() {
                                        @Override
                                        public boolean apply(Integer _) {
                                            return !brokerIds.contains(_);
                                        }
                                    }));
                        }
                        if (isrInfo.size() < inSyncReplicas.size()) {
                            final List<Integer> brokerIds = Utils.mapList(isrInfo, new Function1<Broker, Integer>() {
                                @Override
                                public Integer apply(Broker _) {
                                    return _.id;
                                }
                            });
                            throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                                    Utils.filter(inSyncReplicas, new Predicate<Integer>() {
                                        @Override
                                        public boolean apply(Integer _) {
                                            return !brokerIds.contains(_);
                                        }
                                    }));
                        }

                        return new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError);
                    } catch (Throwable e) {
                        logger.debug("Error while fetching metadata for partition [{},{}]", topic, partition, e);
                        return new PartitionMetadata(partition, leaderInfo, replicaInfo, isrInfo,
                                ErrorMapping.codeFor(e.getClass()));
                    }
                }
            });


            return new TopicMetadata(topic, partitionMetadata);
        } else {
            // topic doesn't exist, send appropriate error code
            return new TopicMetadata(topic, Lists.<PartitionMetadata>newArrayList(), ErrorMapping.UnknownTopicOrPartitionCode);
        }
    }

    private static List<Broker> getBrokerInfoFromCache(final ZkClient zkClient,
                                                       final Map<Integer, Broker> cachedBrokerInfo,
                                                       Collection<Integer> brokerIds) {
        final List<Integer> failedBrokerIds = Lists.newArrayList();
        final Map<Integer, Broker> finalCachedBrokerInfo = cachedBrokerInfo;
        final List<Broker> brokerMetadata = Utils.mapList(brokerIds, new Function1<Integer, Broker>() {
            @Override
            public Broker apply(Integer id) {
                Broker brokerInfo = finalCachedBrokerInfo.get(id);
                if (brokerInfo != null) return brokerInfo;  // return broker info from the cache

                // fetch it from zookeeper
                brokerInfo = ZkUtils.getBrokerInfo(zkClient, id);
                if (brokerInfo != null) {
                    cachedBrokerInfo.put(id, brokerInfo);
                    return brokerInfo;
                }

                failedBrokerIds.add(id);

                return null;
            }
        });

        return Utils.filter(brokerMetadata, new Predicate<Broker>() {
            @Override
            public boolean apply(Broker input) {
                return input != null;
            }
        });

    }

    private static int replicaIndex(int firstReplicaIndex, int secondReplicaShift, int replicaIndex, int nBrokers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (firstReplicaIndex + shift) % nBrokers;
    }
}
