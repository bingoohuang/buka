package kafka.admin;

import com.google.common.base.Predicate;
import com.google.common.collect.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.common.AdminCommandFailedException;
import kafka.common.TopicAndPartition;
import kafka.controller.ReassignedPartitionsContext;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ReassignPartitionsCommand {
    public static void main(String[] args) throws IOException {
        final ReassignPartitionsCommandOptions opts = new ReassignPartitionsCommandOptions(args);

        // should have exactly one action
        int actions = Utils.count(Lists.newArrayList(opts.generateOpt, opts.executeOpt, opts.verifyOpt), new Predicate<OptionSpec<?>>() {
            @Override
            public boolean apply(OptionSpec<?> _) {
                return opts.options.has(_);
            }
        });
        if (actions != 1) {
            opts.parser.printHelpOn(System.err);
            Utils.croak("Command must include exactly one action: --generate, --execute or --verify");
        }

        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt);

        String zkConnect = opts.options.valueOf(opts.zkConnectOpt);
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer.instance);
        try {
            if (opts.options.has(opts.verifyOpt))
                verifyAssignment(zkClient, opts);
            else if (opts.options.has(opts.generateOpt))
                generateAssignment(zkClient, opts);
            else if (opts.options.has(opts.executeOpt))
                executeAssignment(zkClient, opts);
        } catch (Throwable e) {
            System.out.println("Partitions reassignment failed due to " + e.getMessage());
            System.out.println(Utils.stackTrace(e));
        } finally {
            if (zkClient != null)
                zkClient.close();
        }
    }

    public static void verifyAssignment(ZkClient zkClient, ReassignPartitionsCommandOptions opts) throws IOException {
        if (!opts.options.has(opts.reassignmentJsonFileOpt)) {
            opts.parser.printHelpOn(System.err);
            Utils.croak("If --verify option is used, command must include --reassignment-json-file that was used during the --execute option");
        }
        String jsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt);
        String jsonString = Utils.readFileAsString(jsonFile);
        Multimap<TopicAndPartition, Integer> partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentData(jsonString);

        System.out.println("Status of partition reassignment:");
        Map<TopicAndPartition, ReassignmentStatus> reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkClient, partitionsToBeReassigned);
        Utils.foreach(reassignedPartitionsStatus, new Callable2<TopicAndPartition, ReassignmentStatus>() {
            @Override
            public void apply(TopicAndPartition _1, ReassignmentStatus _2) {
                switch (_2) {
                    case ReassignmentCompleted:
                        System.out.println(String.format("Reassignment of partition %s completed successfully", _1));
                        break;
                    case ReassignmentFailed:
                        System.out.println(String.format("Reassignment of partition %s failed", _1));
                        break;
                    case ReassignmentInProgress:
                        System.out.println(String.format("Reassignment of partition %s is still in progress", _1));
                        break;
                }
            }
        });
    }

    public static void generateAssignment(ZkClient zkClient, ReassignPartitionsCommandOptions opts) throws IOException {
        if (!(opts.options.has(opts.topicsToMoveJsonFileOpt) && opts.options.has(opts.brokerListOpt))) {
            opts.parser.printHelpOn(System.err);
            Utils.croak("If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options");
        }
        String topicsToMoveJsonFile = opts.options.valueOf(opts.topicsToMoveJsonFileOpt);
        final List<Integer> brokerListToReassign = Utils.mapList(opts.options.valueOf(opts.brokerListOpt).split(","), new Function1<String, Integer>() {
            @Override
            public Integer apply(String _) {
                return Integer.parseInt(_);
            }
        });
        String topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile);
        List<String> topicsToReassign = ZkUtils.parseTopicsData(topicsToMoveJsonString);
        Multimap<TopicAndPartition, Integer> topicPartitionsToReassign = ZkUtils.getReplicaAssignmentForTopics(zkClient, topicsToReassign);

        final Multimap<TopicAndPartition, Integer> partitionsToBeReassigned = HashMultimap.create();
        Table<String, TopicAndPartition, Collection<Integer>> groupedByTopic = Utils.groupBy(topicPartitionsToReassign, new Function2<TopicAndPartition, Collection<Integer>, String>() {
            @Override
            public String apply(TopicAndPartition _1, Collection<Integer> arg2) {
                return _1.topic;
            }
        });

        Utils.foreach(groupedByTopic, new Callable2<String, Map<TopicAndPartition, Collection<Integer>>>() {
            @Override
            public void apply(final String _1, Map<TopicAndPartition, Collection<Integer>> _2) {
                Multimap<Integer, Integer> assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerListToReassign, _2.size(),
                        Utils.head(_2)._2.size());
                Utils.foreach(assignedReplicas, new Callable2<Integer, Collection<Integer>>() {
                    @Override
                    public void apply(Integer _10, Collection<Integer> _20) {
                        partitionsToBeReassigned.putAll(new TopicAndPartition(_1, _10), _20);
                    }
                });
            }
        });

        Multimap<TopicAndPartition, Integer> currentPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Utils.mapList(partitionsToBeReassigned, new Function2<TopicAndPartition, Collection<Integer>, String>() {
            @Override
            public String apply(TopicAndPartition _1, Collection<Integer> arg2) {
                return _1.topic;
            }
        }));

        System.out.println(String.format("Current partition replica assignment\n\n%s",
                ZkUtils.getPartitionReassignmentZkData(currentPartitionReplicaAssignment)));
        System.out.println(String.format("Proposed partition reassignment configuration\n\n%s", ZkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned)));
    }

    public static void executeAssignment(ZkClient zkClient, ReassignPartitionsCommandOptions opts) throws IOException {
        if (!opts.options.has(opts.reassignmentJsonFileOpt)) {
            opts.parser.printHelpOn(System.err);
            Utils.croak("If --execute option is used, command must include --reassignment-json-file that was output " +
                    "during the --generate option");
        }
        String reassignmentJsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt);
        String reassignmentJsonString = Utils.readFileAsString(reassignmentJsonFile);
        Multimap<TopicAndPartition, Integer> partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentData(reassignmentJsonString);
        if (partitionsToBeReassigned.isEmpty())
            throw new AdminCommandFailedException("Partition reassignment data file %s is empty", reassignmentJsonFile);
        ReassignPartitionsCommand reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, partitionsToBeReassigned);
        // before starting assignment, output the current replica assignment to facilitate rollback
        Multimap<TopicAndPartition, Integer> currentPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Utils.mapList(partitionsToBeReassigned, new Function2<TopicAndPartition, Collection<Integer>, String>() {
            @Override
            public String apply(TopicAndPartition _1, Collection<Integer> arg2) {
                return _1.topic;
            }
        }));
        System.out.println(String.format("Current partition replica assignment\n\n%s\n\nSave this to use as the --reassignment-json-file option during rollback"
                , ZkUtils.getPartitionReassignmentZkData(currentPartitionReplicaAssignment)));
        // start the reassignment
        if (reassignPartitionsCommand.reassignPartitions())
            System.out.println("Successfully started reassignment of partitions %s".format(ZkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned)));
        else
            System.out.println(String.format("Failed to reassign partitions %s", partitionsToBeReassigned));
    }

    private static Map<TopicAndPartition, ReassignmentStatus> checkIfReassignmentSucceeded(final ZkClient zkClient, final Multimap<TopicAndPartition, Integer> partitionsToBeReassigned) {

        final Multimap<TopicAndPartition, Integer> partitionsBeingReassigned = HashMultimap.create();
        Utils.foreach(ZkUtils.getPartitionsBeingReassigned(zkClient), new Callable2<TopicAndPartition, ReassignedPartitionsContext>() {
            @Override
            public void apply(TopicAndPartition topicAndPartition, ReassignedPartitionsContext _) {
                partitionsBeingReassigned.putAll(topicAndPartition, _.newReplicas);
            }
        });

        final Map<TopicAndPartition, ReassignmentStatus> result = Maps.newHashMap();
        Utils.foreach(partitionsToBeReassigned, new Callable2<TopicAndPartition, Collection<Integer>>() {
            @Override
            public void apply(TopicAndPartition _1, Collection<Integer> _2) {
                result.put(_1, checkIfPartitionReassignmentSucceeded(zkClient, _1,
                        _2, partitionsToBeReassigned, partitionsBeingReassigned));
            }
        });
        return result;
    }

    public static ReassignmentStatus checkIfPartitionReassignmentSucceeded(ZkClient zkClient, TopicAndPartition topicAndPartition,
                                                                           Collection<Integer> reassignedReplicas,
                                                                           Multimap<TopicAndPartition, Integer> partitionsToBeReassigned,
                                                                           Multimap<TopicAndPartition, Integer> partitionsBeingReassigned) {
        Collection<Integer> newReplicas = partitionsToBeReassigned.get(topicAndPartition);
        Collection<Integer> partition = partitionsBeingReassigned.get(topicAndPartition);
        if (partition != null) return ReassignmentStatus.ReassignmentInProgress;


        // check if the current replica assignment matches the expected one after reassignment
        List<Integer> assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition);
        if (assignedReplicas.equals(newReplicas))
            return ReassignmentStatus.ReassignmentCompleted;
        else {
            System.out.println(String.format("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
                    " for partition %s", assignedReplicas, newReplicas, topicAndPartition));
            return ReassignmentStatus.ReassignmentFailed;
        }
    }

    public ZkClient zkClient;
    public Multimap<TopicAndPartition, Integer> partitions;

    public ReassignPartitionsCommand(ZkClient zkClient, Multimap<TopicAndPartition, Integer> partitions) {
        this.zkClient = zkClient;
        this.partitions = partitions;
    }

    Logger logger = LoggerFactory.getLogger(ReassignPartitionsCommand.class);

    public boolean reassignPartitions() {
        try {
            Multimap<TopicAndPartition, Integer> validPartitions = Utils.filter(partitions, new Predicate2<TopicAndPartition, Collection<Integer>>() {
                @Override
                public boolean apply(TopicAndPartition _1, Collection<Integer> integers) {
                    return validatePartition(zkClient, _1.topic, _1.partition);
                }
            });
            String jsonReassignmentData = ZkUtils.getPartitionReassignmentZkData(validPartitions);
            ZkUtils.createPersistentPath(zkClient, ZkUtils.ReassignPartitionsPath, jsonReassignmentData);
            return true;
        } catch (ZkNodeExistsException e) {
            Map<TopicAndPartition, ReassignedPartitionsContext> partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient);
            throw new AdminCommandFailedException("Partition reassignment currently in " +
                    "progress for %s. Aborting operation", partitionsBeingReassigned);
        } catch (Throwable e) {
            logger.error("Admin command failed", e);
            return false;
        }
    }

    public boolean validatePartition(ZkClient zkClient, String topic, int partition) {
        // check if partition exists
        Collection<Integer> partitions = ZkUtils.getPartitionsForTopics(zkClient, Lists.newArrayList(topic)).get(topic);
        if (partitions == null) {
            logger.error("Skipping reassignment of partition " +
                    "[{},{}] since topic {} doesn't exist", topic, partition, topic);
            return false;
        }
        if (partitions.contains(partition)) {
            return true;
        } else {
            logger.error("Skipping reassignment of partition [{},{}] since it doesn't exist", topic, partition);
            return false;
        }
    }

    static class ReassignPartitionsCommandOptions {
        public String[] args;

        ReassignPartitionsCommandOptions(String[] args) {
            this.args = args;

            parser = new OptionParser();

            zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
                    "form host:port. Multiple URLS can be given to allow fail-over.")
                    .withRequiredArg()
                    .describedAs("urls")
                    .ofType(String.class);
            generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
                    " Note that this only generates a candidate assignment, it does not execute it.");
            executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.");
            verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the --reassignment-json-file option.");
            reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                    "The format to use is - \n" +
                    "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3] }],\n\"version\":1\n}")
                    .withRequiredArg()
                    .describedAs("manual assignment json file path")
                    .ofType(String.class);
            topicsToMoveJsonFileOpt = parser.accepts("topics-to-move-json-file", "Generate a reassignment configuration to move the partitions" +
                    " of the specified topics to the list of brokers specified by the --broker-list option. The format to use is - \n" +
                    "{\"topics\":\n\t[{\"topic\": \"foo\"},{\"topic\": \"foo1\"}],\n\"version\":1\n}")
                    .withRequiredArg()
                    .describedAs("topics to reassign json file path")
                    .ofType(String.class);
            brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned" +
                    " in the form \"0,1,2\". This is required if --topics-to-move-json-file is used to generate reassignment configuration")
                    .withRequiredArg()
                    .describedAs("brokerlist")
                    .ofType(String.class);

            options = parser.parse(args);
        }

        public OptionParser parser;

        public OptionSpec<String> zkConnectOpt;
        public OptionSpec<?> generateOpt;
        public OptionSpec<?> executeOpt;
        public OptionSpec<?> verifyOpt;
        public OptionSpec<String> reassignmentJsonFileOpt;
        public OptionSpec<String> topicsToMoveJsonFileOpt;
        public OptionSpec<String> brokerListOpt;

        public OptionSet options;
    }

    static enum ReassignmentStatus {
        ReassignmentCompleted {
            @Override
            public int status() {
                return 1;
            }
        }, ReassignmentInProgress {
            @Override
            public int status() {
                return 0;
            }
        }, ReassignmentFailed {
            @Override
            public int status() {
                return -1;
            }
        };

        public abstract int status();
    }
}
