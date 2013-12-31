package kafka.admin;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.common.AdminCommandFailedException;
import kafka.common.TopicAndPartition;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PreferredReplicaLeaderElectionCommand {
    public static void main(String[] args) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
                "for which preferred replica leader election should be done, in the following format - \n" +
                "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
                "Defaults to all existing partitions")
                .withRequiredArg()
                .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
                .ofType(String.class);
        OptionSpec<String> zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
                "form host:port. Multiple URLS can be given to allow fail-over.")
                .withRequiredArg()
                .describedAs("urls")
                .ofType(String.class);

        OptionSet options = parser.parse(args);

        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt);

        String zkConnect = options.valueOf(zkConnectOpt);
        ZkClient zkClient = null;

        try {
            zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer.instance);
            Set<TopicAndPartition> partitionsForPreferredReplicaElection =
                    (!options.has(jsonFileOpt))
                            ? ZkUtils.getAllPartitions(zkClient)
                            : parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt)));
            PreferredReplicaLeaderElectionCommand preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection);

            preferredReplicaElectionCommand.moveLeaderToPreferredReplica();
            System.out.println(String.format("Successfully started preferred replica election for partitions %s", partitionsForPreferredReplicaElection));
        } catch (Throwable e) {
            System.out.println("Failed to start preferred replica election");
            System.out.println(Utils.stackTrace(e));
        } finally {
            if (zkClient != null)
                zkClient.close();
        }
    }


    public static Set<TopicAndPartition> parsePreferredReplicaElectionData(String jsonString) {
        JSONObject m = Json.parseFull(jsonString);
        if (m == null) throw new AdminOperationException("Preferred replica election data is empty");

        JSONArray partitions = m.getJSONArray("partitions");
        if (partitions == null) throw new AdminOperationException("Preferred replica election data is empty");

        return Utils.mapSet(partitions, new Function1<Object, TopicAndPartition>() {
            @Override
            public TopicAndPartition apply(Object arg) {
                JSONObject p = (JSONObject) arg;

                String topic = p.getString("topic");
                int partition = p.getIntValue("partition");
                return new TopicAndPartition(topic, partition);
            }
        });
    }

    public static void writePreferredReplicaElectionData(ZkClient zkClient,
                                                         Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection) {
        String zkPath = ZkUtils.PreferredReplicaLeaderElectionPath;
        Map<String, Object> map = Maps.newHashMap();
        final List<Object> partitionsList = Lists.newArrayList();
        Utils.foreach(partitionsUndergoingPreferredReplicaElection, new Callable1<TopicAndPartition>() {
            @Override
            public void apply(TopicAndPartition e) {
                Map<String, Object> m = Maps.newHashMap();
                partitionsList.add(m);
                m.put("topic", e.topic);
                m.put("partition", e.partition);
            }
        });


        map.put("version", 1);
        map.put("partitions", partitionsList);
        String jsonData = Json.encode(map);
        try {
            ZkUtils.createPersistentPath(zkClient, zkPath, jsonData);
            logger.info("Created preferred replica election path with {}", jsonData);
        } catch (ZkNodeExistsException e) {
            Set<TopicAndPartition> partitionsUndergoingPreferredReplicaElection1 =
                    PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(ZkUtils.readData(zkClient, zkPath)._1);
            throw new AdminOperationException("Preferred replica leader election currently in progress for " +
                    "{}. Aborting operation", partitionsUndergoingPreferredReplicaElection1);
        } catch (Throwable e) {
            throw new AdminOperationException(e.toString());
        }
    }

    static Logger logger = LoggerFactory.getLogger(PreferredReplicaLeaderElectionCommand.class);

    public ZkClient zkClient;
    public Set<TopicAndPartition> partitions;

    public PreferredReplicaLeaderElectionCommand(ZkClient zkClient, Set<TopicAndPartition> partitions) {
        this.zkClient = zkClient;
        this.partitions = partitions;
    }


    public void moveLeaderToPreferredReplica() {
        try {
            Set<TopicAndPartition> validPartitions = Utils.filterSet(partitions, new Predicate<TopicAndPartition>() {
                @Override
                public boolean apply(TopicAndPartition p) {
                    return validatePartition(zkClient, p.topic, p.partition);
                }
            });
            PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions);
        } catch (Throwable e) {
            throw new AdminCommandFailedException(e, "Admin command failed");
        }
    }

    public boolean validatePartition(ZkClient zkClient, String topic, int partition) {
        // check if partition exists
        Collection<Integer> partitions = ZkUtils.getPartitionsForTopics(zkClient, Lists.newArrayList(topic)).get(topic);
        if (partitions == null) {
            logger.error("Skipping preferred replica leader election for partition " +
                    "[{},{}] since topic {} doesn't exist", topic, partition, topic);
            return false;
        }

        if (partitions.contains(partition)) {
            return true;
        }

        logger.error("Skipping preferred replica leader election for partition [{},{}] since it doesn't exist", topic, partition);
        return false;
    }
}
