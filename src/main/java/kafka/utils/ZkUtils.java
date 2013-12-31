package kafka.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.*;
import kafka.admin.AdminOperationException;
import kafka.admin.PreferredReplicaLeaderElectionCommands;
import kafka.api.LeaderAndIsr;
import kafka.cluster.Broker;
import kafka.cluster.Brokers;
import kafka.cluster.Cluster;
import kafka.common.KafkaException;
import kafka.common.NoEpochForPartitionException;
import kafka.common.TopicAndPartition;
import kafka.consumer.TopicCount;
import kafka.consumer.TopicCounts;
import kafka.controller.KafkaControllers;
import kafka.controller.LeaderIsrAndControllerEpoch;
import kafka.controller.PartitionAndReplica;
import kafka.controller.ReassignedPartitionsContext;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class ZkUtils {
    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";
    public static final String TopicConfigPath = "/config/topics";
    public static final String TopicConfigChangesPath = "/config/changes";
    public static final String ControllerPath = "/controller";
    public static final String ControllerEpochPath = "/controller_epoch";
    public static final String ReassignPartitionsPath = "/admin/reassign_partitions";
    public static final String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";

    static Logger logger = LoggerFactory.getLogger(ZkUtils.class);

    public static String getTopicPath(String topic) {
        return BrokerTopicsPath + "/" + topic;
    }

    public static String getTopicPartitionsPath(String topic) {
        return getTopicPath(topic) + "/partitions";
    }

    public static String getTopicConfigPath(String topic) {
        return TopicConfigPath + "/" + topic;
    }

    public static int getController(ZkClient zkClient) {
        String controller = readDataMaybeNull(zkClient, ControllerPath)._1;
        if (controller != null)
            return KafkaControllers.parseControllerId(controller);

        throw new KafkaException("Controller doesn't exist");
    }

    public static String getTopicPartitionPath(String topic, int partitionId) {
        return getTopicPartitionsPath(topic) + "/" + partitionId;
    }

    public static String getTopicPartitionLeaderAndIsrPath(String topic, int partitionId) {
        return getTopicPartitionPath(topic, partitionId) + "/" + "state";
    }

    public static List<Integer> getSortedBrokerList(ZkClient zkClient) {
        List<String> children = ZkUtils.getChildren(zkClient, BrokerIdsPath);

        List<Integer> sorted = Lists.newArrayList();
        for (String child : children) {
            sorted.add(Integer.parseInt(child));
        }

        Collections.sort(sorted);

        return sorted;
    }

    public static List<Broker> getAllBrokersInCluster(ZkClient zkClient) {
        List<String> brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);

        List<Broker> brokers = Lists.newArrayList();
        if (brokerIds == null) return brokers;

        Collections.sort(brokerIds);

        for (String brokerId : brokerIds) {
            int brokerInt = Integer.parseInt(brokerId);
            Broker brokerInfo = getBrokerInfo(zkClient, brokerInt);
            if (brokerInfo != null) brokers.add(brokerInfo);
        }

        return brokers;
    }

    public static LeaderIsrAndControllerEpoch getLeaderIsrAndEpochForPartition(ZkClient zkClient, String topic, int partition) {
        String leaderAndIsrPath = getTopicPartitionLeaderAndIsrPath(topic, partition);
        Tuple2<String, Stat> leaderAndIsrInfo = readDataMaybeNull(zkClient, leaderAndIsrPath);
        String leaderAndIsrStr = leaderAndIsrInfo._1;
        Stat stat = leaderAndIsrInfo._2;

        if (leaderAndIsrStr == null) return null;

        return parseLeaderAndIsr(leaderAndIsrStr, topic, partition, stat);
    }

    public static LeaderAndIsr getLeaderAndIsrForPartition(ZkClient zkClient, String topic, int partition) {
        return getLeaderIsrAndEpochForPartition(zkClient, topic, partition).leaderAndIsr;
    }

    public static void setupCommonPaths(ZkClient zkClient) {
        for (String path : ImmutableList.of(ConsumersPath, BrokerIdsPath, BrokerTopicsPath, TopicConfigChangesPath, TopicConfigPath))
            makeSurePersistentPathExists(zkClient, path);
    }

    public static LeaderIsrAndControllerEpoch parseLeaderAndIsr(String leaderAndIsrStr, String topic, int partition, Stat stat) {
        JSONObject leaderIsrAndEpochInfo = Json.parseFull(leaderAndIsrStr);
        if (leaderIsrAndEpochInfo == null) return null;

        int leader = leaderIsrAndEpochInfo.getIntValue("leader");
        int epoch = leaderIsrAndEpochInfo.getIntValue("leader_epoch");

        List<Integer> isr = (List<Integer>) leaderIsrAndEpochInfo.get("isr");
        int controllerEpoch = leaderIsrAndEpochInfo.getIntValue("controller_epoch");
        int zkPathVersion = stat.getVersion();
        logger.debug("Leader {}, Epoch {}, Isr {}, Zk path version {} for partition [{},{}]", leader, epoch,
                isr.toString(), zkPathVersion, topic, partition);
        return new LeaderIsrAndControllerEpoch(new LeaderAndIsr(leader, epoch, isr, zkPathVersion), controllerEpoch);

    }

    public static Integer getLeaderForPartition(ZkClient zkClient, String topic, int partition) {
        String leaderAndIsr = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        if (leaderAndIsr == null) return null;

        JSONObject m = Json.parseFull(leaderAndIsr);
        if (m == null) return null;

        return m.getIntValue("leader");
    }

    /**
     * This API should read the epoch in the ISR path. It is sufficient to read the epoch in the ISR path, since if the
     * leader fails after updating epoch in the leader path and before updating epoch in the ISR path, effectively some
     * other broker will retry becoming leader with the same new epoch value.
     */
    public static int getEpochForPartition(ZkClient zkClient, String topic, int partition) {
        String leaderAndIsr = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        if (leaderAndIsr == null)
            throw new NoEpochForPartitionException("No epoch, ISR path for partition [%s,%d] is empty"
                    , topic, partition);

        JSONObject m = Json.parseFull(leaderAndIsr);
        if (m == null)
            throw new NoEpochForPartitionException("No epoch, leaderAndISR data for partition [%s,%d] is invalid", topic, partition);


        return m.getIntValue("leader_epoch");
    }

    /**
     * Gets the in-sync replicas (ISR) for a specific topic and partition
     */
    public static List<Integer> getInSyncReplicasForPartition(ZkClient zkClient, String topic, int partition) {
        String leaderAndIsr = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1;
        List<Integer> isr = Lists.newArrayList();
        if (leaderAndIsr == null) return isr;


        JSONObject m = Json.parseFull(leaderAndIsr);
        if (m == null) return isr;

        return (List<Integer>) m.get("isr");
    }

    /**
     * Gets the assigned replicas (AR) for a specific topic and partition
     */
    public static List<Integer> getReplicasForPartition(ZkClient zkClient, String topic, int partition) {
        String jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
        List<Integer> ar = Lists.newArrayList();
        if (jsonPartitionMap == null) return ar;


        JSONObject m = Json.parseFull(jsonPartitionMap);
        if (m == null) return ar;

        JSONObject replicaMap = m.getJSONObject("partitions");
        if (replicaMap == null) return ar;

        return (List<Integer>) replicaMap.get("" + partition);
    }

    public static boolean isPartitionOnBroker(ZkClient zkClient, String topic, int partition, int brokerId) {
        List<Integer> replicas = getReplicasForPartition(zkClient, topic, partition);
        logger.debug("The list of replicas for partition [{},{}] is {}", topic, partition, replicas);
        return replicas.contains(brokerId + "");
    }

    public static void registerBrokerInZk(ZkClient zkClient, int id, String host, int port, int timeout, int jmxPort) {
        String brokerIdPath = ZkUtils.BrokerIdsPath + "/" + id;
        String timestamp = SystemTime.instance.milliseconds() + "";
        String brokerInfo = Json.encode(ImmutableMap.of("version", 1, "host", host, "port", port, "jmx_port", jmxPort, "timestamp", timestamp));
        Broker expectedBroker = new Broker(id, host, port);

        try {
            createEphemeralPathExpectConflictHandleZKBug(zkClient, brokerIdPath, brokerInfo, expectedBroker,
                    new Function2<String, Object, Boolean>() {
                        @Override
                        public Boolean apply(String brokerString, Object broker) {
                            return Brokers.createBroker(((Broker) broker).id, brokerString).equals(broker);
                        }
                    }, timeout);

        } catch (ZkNodeExistsException e) {
            throw new RuntimeException("A broker is already registered on the path " + brokerIdPath
                    + ". This probably " + "indicates that you either have configured a brokerid that is already in use, or "
                    + "else you have shutdown this broker and restarted it faster than the zookeeper "
                    + "timeout so it appears to be re-registering.");
        }
        logger.info("Registered broker {} at path {} with address {}:{}.", id, brokerIdPath, host, port);
    }

    public static String getConsumerPartitionOwnerPath(String group, String topic, int partition) {
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
        return topicDirs.consumerOwnerDir() + "/" + partition;
    }


    public static String leaderAndIsrZkData(LeaderAndIsr leaderAndIsr, int controllerEpoch) {
        return Json.encode(ImmutableMap.of("version", 1, "leader", leaderAndIsr.leader, "leader_epoch", leaderAndIsr.leaderEpoch,
                "controller_epoch", controllerEpoch, "isr", leaderAndIsr.isr));
    }

    /**
     * Get JSON partition to replica map from zookeeper.
     */
    public static String replicaAssignmentZkData(Multimap<String, Integer> map) {
        return Json.encode(ImmutableMap.of("version", 1, "partitions", map));
    }

    /**
     * make sure a persistent path exists in ZK. Create the path if not exist.
     */
    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path))
            client.createPersistent(path, true); // won't throw NoNodeException or NodeExistsException
    }

    /**
     * create the parent path
     */
    private static void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0)
            client.createPersistent(parentDir, true);
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     */
    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistException if node already exists.
     */
    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // this can happen when there is connection loss; make sure the data is what we intend to write
            String storedData = null;
            try {
                storedData = readData(client, path)._1;
            } catch (ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller handles this
            }
            if (storedData == null || storedData != data) {
                logger.info("conflict in {} data: {} stored data: {}", path, data, storedData);
                throw e;
            } else {
                // otherwise, the creation succeeded, return normally
                logger.info("{} exists with value {} during connection loss; this is ok", path, data);
            }
        }
    }

    /**
     * Create an ephemeral node with the given path and data.
     * Throw NodeExistsException if node already exists.
     * Handles the following ZK session timeout bug:
     * <p/>
     * https://issues.apache.org/jira/browse/ZOOKEEPER-1740
     * <p/>
     * Upon receiving a NodeExistsException, read the data from the conflicted path and
     * trigger the checker function comparing the read data and the expected data,
     * If the checker function returns true then the above bug might be encountered, back off and retry;
     * otherwise re-throw the exception
     */
    public static void createEphemeralPathExpectConflictHandleZKBug(ZkClient zkClient, String path, String data,
                                                                    Object expectedCallerData,
                                                                    Function2<String, Object, Boolean> checker,
                                                                    int backoffTime) {
        while (true) {
            try {
                createEphemeralPathExpectConflict(zkClient, path, data);
                return;
            } catch (ZkNodeExistsException e) {
                // An ephemeral node may still exist even after its corresponding session has expired
                // due to a Zookeeper bug, in this case we need to retry writing until the previous node is deleted
                // and hence the write succeeds without ZkNodeExistsException
                String writtenData = ZkUtils.readDataMaybeNull(zkClient, path)._1;
                if (writtenData == null) {
                    // the node disappeared; retry creating the ephemeral node immediately
                } else {
                    if (checker.apply(writtenData, expectedCallerData)) {
                        logger.info("I wrote this conflicted ephemeral node [{}] at {} a while back in a different session, "
                                + "hence I will backoff for this node to be deleted by Zookeeper and retry", data, path);
                        SystemTime.instance.sleep(backoffTime);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Create an persistent node with the given path and data. Create parents if necessary.
     */
    public static void createPersistentPath(ZkClient client, String path) {
        createPersistentPath(client, path, "");
    }

    public static void createPersistentPath(ZkClient client, String path, String data) {
        try {
            client.createPersistent(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
    }

    public static String createSequentialPersistentPath(ZkClient client, String path) {
        return createSequentialPersistentPath(client, path, "");
    }

    public static String createSequentialPersistentPath(ZkClient client, String path, String data) {
        return client.createPersistentSequential(path, data);
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     * Return the updated path zkVersion
     */
    public static void updatePersistentPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            try {
                client.createPersistent(path, data);
            } catch (ZkNodeExistsException e1) {
                client.writeData(path, data);
            }
        }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the path doesn't
     * exist, the current version is not the expected version, etc.) return (false, -1)
     */
    public static Tuple2<Boolean, Integer> conditionalUpdatePersistentPath(ZkClient client, String path, String data, int expectVersion) {
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            logger.debug("Conditional update of path {} with value {} and expected version {} succeeded, returning the new version: {}"
                    , path, data, expectVersion, stat.getVersion());
            return Tuple2.make(true, stat.getVersion());
        } catch (Exception e) {
            logger.error("Conditional update of path {} with data {} and expected version {} failed due to {}", path, data,
                    expectVersion, e.getMessage());
            return Tuple2.make(false, -1);
        }
    }

    /**
     * Conditional update the persistent path data, return (true, newVersion) if it succeeds, otherwise (the current
     * version is not the expected version, etc.) return (false, -1). If path doesn't exist, throws ZkNoNodeException
     */
    public static Tuple2<Boolean, Integer> conditionalUpdatePersistentPathIfExists(ZkClient client, String path, String data, int expectVersion) {
        try {
            Stat stat = client.writeDataReturnStat(path, data, expectVersion);
            logger.debug("Conditional update of path {} with value {} and expected version {} succeeded, returning the new version: {}"
                    , path, data, expectVersion, stat.getVersion());
            return Tuple2.make(true, stat.getVersion());
        } catch (ZkNoNodeException nne) {
            throw nne;
        } catch (Exception e) {
            logger.error("Conditional update of path {} with data {} and expected version {} failed due to {}", path, data,
                    expectVersion, e.getMessage());
            return Tuple2.make(false, -1);
        }
    }

    /**
     * Update the value of a persistent node with the given path and data.
     * create parrent directory if necessary. Never throw NodeExistException.
     */
    public static void updateEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    public static boolean deletePath(ZkClient client, String path) {
        try {
            return client.delete(path);
        } catch (ZkNoNodeException e) {
            // this can happen during a connection loss event, return normally
            logger.info("{} deleted during connection loss; this is ok", path);
            return false;
        }
    }

    public static void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch (ZkNoNodeException e) {
            // this can happen during a connection loss event, return normally
            logger.info(path + " deleted during connection loss; this is ok");
        }
    }

    public static void maybeDeletePath(String zkUrl, String dir) {
        try {
            ZkClient zk = new ZkClient(zkUrl, 30 * 1000, 30 * 1000, ZKStringSerializer.instance);
            zk.deleteRecursive(dir);
            zk.close();
        } catch (Throwable e) {
            // swallow
        }
    }

    public static Tuple2<String, Stat> readData(ZkClient client, String path) {
        Stat stat = new Stat();
        String dataStr = client.readData(path, stat);
        return Tuple2.make(dataStr, stat);
    }

    public static Tuple2<String, Stat> readDataMaybeNull(ZkClient client, String path) {
        Stat stat = new Stat();
        try {
            return Tuple2.make((String) client.readData(path, stat), stat);
        } catch (ZkNoNodeException e) {
            return Tuple2.make(null, stat);
        }
    }

    public static List<String> getChildren(ZkClient client, String path) {
        // triggers implicit conversion from java list to scala Seq
        return client.getChildren(path);
    }

    public static List<String> getChildrenParentMayNotExist(ZkClient client, String path) {
        // triggers implicit conversion from java list to scala Seq
        try {
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    /**
     * Check if the given path exists
     */
    public static boolean pathExists(ZkClient client, String path) {
        return client.exists(path);
    }

    public static String getLastPart(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static Cluster getCluster(ZkClient zkClient) {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            String brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node)._1;
            cluster.add(Brokers.createBroker(Integer.parseInt(node), brokerZKString));
        }

        return cluster;
    }

    public static Map<TopicAndPartition, LeaderIsrAndControllerEpoch>
    getPartitionLeaderAndIsrForTopics(ZkClient zkClient, Set<TopicAndPartition> topicAndPartitions) {
        Map<TopicAndPartition, LeaderIsrAndControllerEpoch> ret = Maps.newHashMap();
        for (TopicAndPartition topicAndPartition : topicAndPartitions) {
            LeaderIsrAndControllerEpoch leaderIsrAndControllerEpoch = ZkUtils.getLeaderIsrAndEpochForPartition(zkClient,
                    topicAndPartition.topic, topicAndPartition.partition);
            if (leaderIsrAndControllerEpoch != null)
                ret.put(topicAndPartition, leaderIsrAndControllerEpoch);
        }

        return ret;
    }

    public static Multimap<TopicAndPartition, Integer> getReplicaAssignmentForTopics(ZkClient zkClient, List<String> topics) {
        Multimap<TopicAndPartition, Integer> ret = HashMultimap.create();

        for (String topic : topics) {
            String jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
            if (jsonPartitionMap == null) continue;

            JSONObject m = Json.parseFull(jsonPartitionMap);
            if (m == null) continue;

            Map<String, Object> repl = (Map<String, Object>) m.get("partitions");
            for (Map.Entry<String, Object> entry : repl.entrySet()) {
                String partition = entry.getKey();
                List<Integer> replicas = (List<Integer>) entry.getValue();

                ret.putAll(new TopicAndPartition(topic, Integer.parseInt(partition)), replicas);
                logger.debug("Replicas assigned to topic [{}], partition [{}] are [{}]", topic, partition, replicas);
            }

        }
        return ret;
    }

    public static Map<String, Multimap<Integer, Integer>> getPartitionAssignmentForTopics(ZkClient zkClient, List<String> topics) {
        Map<String, Multimap<Integer, Integer>> ret = Maps.newHashMap();

        for (String topic : topics) {
            Multimap<Integer, Integer> partitionMap = HashMultimap.create();
            ret.put(topic, partitionMap);

            String jsonPartitionMap = readDataMaybeNull(zkClient, getTopicPath(topic))._1;
            if (jsonPartitionMap != null) {
                JSONObject m = Json.parseFull(jsonPartitionMap);
                Map<String, Object> partitions = (Map<String, Object>) m.get("partitions");
                if (partitions != null) {
                    for (Map.Entry<String, Object> entry : partitions.entrySet()) {
                        partitionMap.putAll(Integer.parseInt(entry.getKey()),
                                (List<Integer>) entry.getValue());
                    }
                }
            }

            logger.debug("Partition map for /brokers/topics/{} is {}", topic, partitionMap);
        }

        return ret;
    }

    public static Multimap<Tuple2<String, Integer>, Integer>
    getReplicaAssignmentFromPartitionAssignment(Map<String, Multimap<Integer, Integer>> topicPartitionAssignment) {
        Multimap<Tuple2<String, Integer>, Integer> ret = HashMultimap.create();

        for (String topic : topicPartitionAssignment.keySet()) {
            Multimap<Integer, Integer> partitionAssignment = topicPartitionAssignment.get(topic);
            for (Integer partition : partitionAssignment.keySet()) {
                Collection<Integer> assignment = partitionAssignment.get(partition);
                ret.putAll(Tuple2.make(topic, partition), assignment);
            }
        }

        return ret;
    }

    public static Multimap<String, Integer> getPartitionsForTopics(ZkClient zkClient, List<String> topics) {
        Map<String, Multimap<Integer, Integer>> partitionAssignmentForTopics = getPartitionAssignmentForTopics(zkClient, topics);

        Multimap<String, Integer> ret = HashMultimap.create();
        for (Map.Entry<String, Multimap<Integer, Integer>> entry : partitionAssignmentForTopics.entrySet()) {
            String topic = entry.getKey();
            Multimap<Integer, Integer> partitionMap = entry.getValue();

            logger.debug("partition assignment of /brokers/topics/{} is {}", topic, partitionMap);

            ret.putAll(topic, partitionMap.keySet());
        }

        return ret;
    }

    public static List<Tuple2<String, Integer>> getPartitionsAssignedToBroker(ZkClient zkClient,
                                                                              List<String> topics,
                                                                              int brokerId) {
        Map<String, Multimap<Integer, Integer>> topicsAndPartitions
                = getPartitionAssignmentForTopics(zkClient, topics);

        List<Tuple2<String, Integer>> ret = Lists.newArrayList();

        for (Map.Entry<String, Multimap<Integer, Integer>> entry : topicsAndPartitions.entrySet()) {
            String topic = entry.getKey();
            Multimap<Integer, Integer> partitionMap = entry.getValue();

            for (Integer partition : partitionMap.keySet()) {
                Collection<Integer> brokerIds = partitionMap.get(partition);
                if (brokerIds.contains(brokerId)) ret.add(Tuple2.make(topic, partition));
            }


        }

        return ret;
    }

    public static Map<TopicAndPartition, ReassignedPartitionsContext> getPartitionsBeingReassigned(ZkClient zkClient) {
        // read the partitions and their new replica list
        String jsonPartitionMap = readDataMaybeNull(zkClient, ReassignPartitionsPath)._1;

        Map<TopicAndPartition, ReassignedPartitionsContext> ret = Maps.newHashMap();
        if (jsonPartitionMap == null) return ret;


        Multimap<TopicAndPartition, Integer> reassignedPartitions = parsePartitionReassignmentData(jsonPartitionMap);
        for (TopicAndPartition topicAndPartition : reassignedPartitions.keySet()) {
            Collection<Integer> partitions = reassignedPartitions.get(topicAndPartition);
            ret.put(topicAndPartition, new ReassignedPartitionsContext(partitions, null));
        }

        return ret;
    }

    public static Multimap<TopicAndPartition, Integer> parsePartitionReassignmentData(String jsonData) {
        Multimap<TopicAndPartition, Integer> reassignedPartitions = HashMultimap.create();


        JSONObject m = Json.parseFull(jsonData);
        if (m == null) return reassignedPartitions;

        List<Object> partitionsSeq = m.getJSONArray("partitions");
        if (partitionsSeq == null) return reassignedPartitions;

        for (Object partitions : partitionsSeq) {
            JSONObject p = (JSONObject) partitions;
            String topic = p.getString("topic");
            int partition = p.getIntValue("partition");
            List<Object> newReplicas = p.getJSONArray("replicas");

            for (Object newReplica : newReplicas) {
                reassignedPartitions.put(new TopicAndPartition(topic, partition), (Integer) newReplica);
            }
        }

        return reassignedPartitions;
    }

    public static List<String> parseTopicsData(String jsonData) {
        List<String> topics = Lists.newArrayList();

        JSONObject m = Json.parseFull(jsonData);
        if (m == null) return topics;

        JSONArray topicsList = m.getJSONArray("topics");
        if (topicsList == null) return topics;

        for (Object topic : topicsList) {
            List<Map<String, Object>> mapPartitionSeq = (List<Map<String, Object>>) topic;
            for (Map<String, Object> p : mapPartitionSeq) {
                String t = (String) p.get("topic");
                topics.add(t);
            }
        }

        return topics;
    }

    public static String getPartitionReassignmentZkData(Multimap<TopicAndPartition, Integer> partitionsToBeReassigned) {
        Map<String, Object> partitions = Maps.newHashMap();

        for (TopicAndPartition topicAndPartition : partitionsToBeReassigned.keySet()) {
            Collection<Integer> replicasCol = partitionsToBeReassigned.get(topicAndPartition);
            partitions.put("topic", topicAndPartition.topic);
            partitions.put("partition", topicAndPartition.partition);
            partitions.put("replicas", replicasCol);
        }

        return Json.encode(ImmutableMap.of("version", 1, "partitions", partitions));
    }

    public static void updatePartitionReassignmentData(ZkClient zkClient, Multimap<TopicAndPartition, Integer> partitionsToBeReassigned) {
        String zkPath = ZkUtils.ReassignPartitionsPath;
        int size = partitionsToBeReassigned.size();
        switch (size) {
            case 0: // need to delete the /admin/reassign_partitions path
                deletePath(zkClient, zkPath);
                logger.info("No more partitions need to be reassigned. Deleting zk path {}", zkPath);
                break;
            default:
                String jsonData = getPartitionReassignmentZkData(partitionsToBeReassigned);
                try {
                    updatePersistentPath(zkClient, zkPath, jsonData);
                    logger.info("Updated partition reassignment path with {}", jsonData);
                } catch (ZkNoNodeException nne) {
                    ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
                    logger.debug("Created path {} with {} for partition reassignment", zkPath, jsonData);
                } catch (Throwable e) {
                    throw new AdminOperationException(e.toString());
                }
        }
    }

    public static Set<PartitionAndReplica> getAllReplicasOnBroker(ZkClient zkClient, List<String> topics, List<Integer> brokerIds) {
        Set<PartitionAndReplica> ret = Sets.newHashSet();

        for (Integer brokerId : brokerIds) {
            // read all the partitions and their assigned replicas into a map organized by
            // { replica id -> partition 1, partition 2...
            List<Tuple2<String, Integer>> partitionsAssignedToThisBroker
                    = getPartitionsAssignedToBroker(zkClient, topics, brokerId);
            if (partitionsAssignedToThisBroker.size() == 0)
                logger.info("No state transitions triggered since no partitions are assigned to brokers {}", brokerIds);

            for (Tuple2<String, Integer> p : partitionsAssignedToThisBroker) {
                ret.add(new PartitionAndReplica(p._1, p._2, brokerId));
            }

        }

        return ret;
    }

    public static Set<TopicAndPartition> getPartitionsUndergoingPreferredReplicaElection(ZkClient zkClient) {
        // read the partitions and their new replica list
        String jsonPartitionList = readDataMaybeNull(zkClient, PreferredReplicaLeaderElectionPath)._1;

        Set<TopicAndPartition> ret = Sets.newHashSet();

        if (jsonPartitionList == null) return ret;

        return PreferredReplicaLeaderElectionCommands.parsePreferredReplicaElectionData(jsonPartitionList);
    }

    public static void deletePartition(ZkClient zkClient, int brokerId, String topic) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        zkClient.delete(brokerIdPath);
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        zkClient.delete(brokerPartTopicPath);
    }

    public static List<String> getConsumersInGroup(ZkClient zkClient, String group) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        return getChildren(zkClient, dirs.consumerRegistryDir());
    }

    public static Multimap<String, String> getConsumersPerTopic(ZkClient zkClient, String group) {
        ZKGroupDirs dirs = new ZKGroupDirs(group);
        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir());
        Multimap<String, String> consumersPerTopicMap = HashMultimap.create();
        for (String consumer : consumers) {
            TopicCount topicCount = TopicCounts.constructTopicCount(group, consumer, zkClient);
            for (Map.Entry<String, Set<String>> entry : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
                String topic = entry.getKey();
                Set<String> consumerThreadIdSet = entry.getValue();
                consumersPerTopicMap.putAll(topic, consumerThreadIdSet);
            }
        }

        return consumersPerTopicMap;
    }

    /**
     * This API takes in a broker id, queries zookeeper for the broker metadata and returns the metadata for that broker
     * or throws an exception if the broker dies before the query to zookeeper finishes
     *
     * @param brokerId The broker id
     * @param zkClient The zookeeper client connection
     * @return An optional Broker object encapsulating the broker metadata
     */
    public static Broker getBrokerInfo(ZkClient zkClient, int brokerId) {
        String brokerInfo = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1;
        if (brokerInfo == null) return null;

        return Brokers.createBroker(brokerId, brokerInfo);
    }

    public static Set<String> getAllTopics(ZkClient zkClient) {
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        if (topics == null)
            return Sets.newHashSet();
        else
            return Sets.newHashSet(topics);
    }

    public static Set<TopicAndPartition> getAllPartitions(ZkClient zkClient) {
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, BrokerTopicsPath);
        Set<TopicAndPartition> ret = Sets.newHashSet();
        if (topics == null) return ret;

        for (String topic : topics) {
            List<String> partitions = getChildren(zkClient, getTopicPartitionsPath(topic));
            for (String partition : partitions) {
                ret.add(new TopicAndPartition(topic, Integer.parseInt(partition)));
            }
        }

        return ret;
    }

}
