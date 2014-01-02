package kafka.admin;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.cluster.Broker;
import kafka.cluster.LogConfigs;
import kafka.common.TopicAndPartition;
import kafka.consumer.Whitelist;
import kafka.utils.*;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

public class TopicCommand {
    public static void main(String[] args) throws IOException {
        final TopicCommandOptions opts = new TopicCommandOptions(args);

        // should have exactly one action
        int actions = Utils.count(Lists.newArrayList(opts.createOpt, opts.deleteOpt, opts.listOpt, opts.alterOpt, opts.describeOpt), new Predicate<OptionSpec<?>>() {
            @Override
            public boolean apply(OptionSpec<?> _) {
                return opts.options.has(_);
            }
        });
        if (actions != 1) {
            System.err.println("Command must include exactly one action: --list, --describe, --create, --delete, or --alter");
            opts.parser.printHelpOn(System.err);
            System.exit(1);
        }

        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt);

        ZkClient zkClient = new ZkClient(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, ZKStringSerializer.instance);

        try {
            if (opts.options.has(opts.createOpt))
                createTopic(zkClient, opts);
            else if (opts.options.has(opts.alterOpt))
                alterTopic(zkClient, opts);
            else if (opts.options.has(opts.deleteOpt))
                deleteTopic(zkClient, opts);
            else if (opts.options.has(opts.listOpt))
                listTopics(zkClient, opts);
            else if (opts.options.has(opts.describeOpt))
                describeTopic(zkClient, opts);
        } catch (Throwable e) {
            System.out.println("Error while executing topic command: " + e.toString());
        } finally {
            zkClient.close();
        }
    }

    private static List<String> getTopics(ZkClient zkClient, TopicCommandOptions opts) {
        String topicsSpec = opts.options.valueOf(opts.topicOpt);
        final Whitelist topicsFilter = new Whitelist(topicsSpec);
        Set<String> allTopics = ZkUtils.getAllTopics(zkClient);
        final List<String> result = Lists.newArrayList();

        Utils.foreach(allTopics, new Callable1<String>() {
            @Override
            public void apply(String topic) {
                if (topicsFilter.isTopicAllowed(topic))
                    result.add(topic);
            }
        });

        Collections.sort(result);
        return result;
    }

    public static void createTopic(ZkClient zkClient, TopicCommandOptions opts) {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt);
        String topic = opts.options.valueOf(opts.topicOpt);
        Properties configs = parseTopicConfigsToBeAdded(opts);
        if (opts.options.has(opts.replicaAssignmentOpt)) {
            Multimap<Integer, Integer> assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt));
            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, assignment, configs);
        } else {
            CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt);
            int partitions = opts.options.valueOf(opts.partitionsOpt).intValue();
            int replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue();
            AdminUtils.createTopic(zkClient, topic, partitions, replicas, configs);
        }
        System.out.println(String.format("Created topic \"%s\".", topic));
    }

    public static void alterTopic(final ZkClient zkClient, final TopicCommandOptions opts) {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt);
        List<String> topics = getTopics(zkClient, opts);
        Utils.foreach(topics, new Callable1<String>() {
            @Override
            public void apply(String topic) {
                if (opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {
                    Properties configsToBeAdded = parseTopicConfigsToBeAdded(opts);
                    List<String> configsToBeDeleted = parseTopicConfigsToBeDeleted(opts);
                    // compile the final set of configs
                    final Properties configs = AdminUtils.fetchTopicConfig(zkClient, topic);
                    configs.putAll(configsToBeAdded);
                    Utils.foreach(configsToBeDeleted, new Callable1<String>() {
                        @Override
                        public void apply(String config) {
                            configs.remove(config);
                        }
                    });
                    AdminUtils.changeTopicConfig(zkClient, topic, configs);
                    System.out.println(String.format("Updated config for topic \"%s\".", topic));
                }
                if (opts.options.has(opts.partitionsOpt)) {
                    System.out.println("WARNING: If partitions are increased for a topic that has a key, the partition " +
                            "logic or ordering of the messages will be affected");
                    int nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue();
                    String replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt);
                    AdminUtils.addPartitions(zkClient, topic, nPartitions, replicaAssignmentStr);
                    System.out.println("adding partitions succeeded!");
                }
                if (opts.options.has(opts.replicationFactorOpt))
                    Utils.croak("Changing the replication factor is not supported.");
            }
        });
    }

    public static void deleteTopic(final ZkClient zkClient, TopicCommandOptions opts) {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt);
        List<String> topics = getTopics(zkClient, opts);
        Utils.foreach(topics, new Callable1<String>() {
            @Override
            public void apply(String topic) {
                AdminUtils.deleteTopic(zkClient, topic);
                System.out.println(String.format("Topic \"%s\" deleted.", topic));
            }
        });
    }

    public static void listTopics(final ZkClient zkClient, TopicCommandOptions opts) {
        if (opts.options.has(opts.topicsWithOverridesOpt)) {
            List<String> allTopics = Lists.newArrayList(ZkUtils.getAllTopics(zkClient));
            Collections.sort(allTopics);
            Utils.foreach(allTopics, new Callable1<String>() {
                @Override
                public void apply(String topic) {
                    Properties configs = AdminUtils.fetchTopicConfig(zkClient, topic);
                    if (configs.size() != 0) {
                        Multimap<TopicAndPartition, Integer> replicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, Lists.newArrayList(topic));
                        int numPartitions = replicaAssignment.size();
                        int replicationFactor = Utils.head(replicaAssignment)._2.size();
                        System.out.println(String.format("\nTopic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s", topic, numPartitions,
                                replicationFactor, configs));
                    }
                }
            });
        } else {
            List<String> allTopics = Lists.newArrayList(ZkUtils.getAllTopics(zkClient));
            Collections.sort(allTopics);
            Utils.foreach(allTopics, new Callable1<String>() {
                @Override
                public void apply(String topic) {
                    System.out.println(topic);
                }
            });
        }
    }

    public static void describeTopic(ZkClient zkClient, TopicCommandOptions opts) {
        List<String> topics = getTopics(zkClient, opts);
        boolean reportUnderReplicatedPartitions = opts.options.has(opts.reportUnderReplicatedPartitionsOpt);
        boolean reportUnavailablePartitions = opts.options.has(opts.reportUnavailablePartitionsOpt);
        Set<Integer> liveBrokers = Utils.mapSet(ZkUtils.getAllBrokersInCluster(zkClient), new Function1<Broker, Integer>() {
            @Override
            public Integer apply(Broker _) {
                return _.id;
            }
        });
        for (String topic : topics) {
            Multimap<Integer, Integer> topicPartitionAssignment = ZkUtils.getPartitionAssignmentForTopics(zkClient, Lists.newArrayList(topic)).get(topic);
            if (topicPartitionAssignment == null) {
                System.out.println("topic " + topic + " doesn't exist!");
                return;
            }

            List<Tuple2<Integer, Collection<Integer>>> sortedPartitions = Utils.toList(topicPartitionAssignment);
            Collections.sort(sortedPartitions, new Comparator<Tuple2<Integer, Collection<Integer>>>() {
                @Override
                public int compare(Tuple2<Integer, Collection<Integer>> m1, Tuple2<Integer, Collection<Integer>> m2) {
                    if (m1._1 < m2._1) return -1;
                    if (m1._1 > m2._1) return 1;
                    return 0;
                }
            });
            if (!reportUnavailablePartitions && !reportUnderReplicatedPartitions) {
                System.out.println(topic);
                Properties config = AdminUtils.fetchTopicConfig(zkClient, topic);
                System.out.println(String.format("\tconfigs: " + config));
                System.out.println(String.format("\tpartitions: " + sortedPartitions.size()));
            }
            for (Tuple2<Integer, Collection<Integer>> tuple2 : sortedPartitions) {
                Integer partitionId = tuple2._1;
                Collection<Integer> assignedReplicas = tuple2._2;


                List<Integer> inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionId);
                Integer leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionId);
                if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
                        (reportUnderReplicatedPartitions && inSyncReplicas.size() < assignedReplicas.size()) ||
                        (reportUnavailablePartitions && (leader != null || !liveBrokers.contains(leader)))) {
                    System.out.print("\t\ttopic: " + topic);
                    System.out.print("\tpartition: " + partitionId);
                    System.out.print("\tleader: " + ((leader != null) ? leader : "none"));
                    System.out.print("\treplicas: " + assignedReplicas);
                    System.out.println("\tisr: " + inSyncReplicas);
                }

            }
        }
    }

    public static Properties parseTopicConfigsToBeAdded(TopicCommandOptions opts) {
        List<String> strings = opts.options.valuesOf(opts.configOpt);
        Map<String, String> configsToBeAdded = Utils.map(strings, new Function1<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> apply(String arg) {
                String[] config = arg.split("\\s*=\\s*");
                checkState(config.length == 2,
                        "Invalid topic config: all configs to be added must be in the format \"key=val\".");
                return Tuple2.make(config[0].trim(), config[1].trim());
            }
        });
        Properties props = new Properties();
        props.putAll(configsToBeAdded);
        LogConfigs.validate(props);
        return props;
    }

    public static List<String> parseTopicConfigsToBeDeleted(TopicCommandOptions opts) {
        List<String> strings = opts.options.valuesOf(opts.deleteConfigOpt);
        List<String[]> configsToBeDeleted = Utils.mapList(strings, new Function1<String, String[]>() {
            @Override
            public String[] apply(String arg) {
                return arg.split("\\s*=\\s*");
            }
        });
        if (opts.options.has(opts.createOpt)) {
            checkState(configsToBeDeleted.size() == 0, "Invalid topic config: all configs on create topic must be in the format \"key=val\".");
        }

        Utils.foreach(configsToBeDeleted, new Callable1<String[]>() {
            @Override
            public void apply(String[] config) {
                checkState(config.length == 1,
                        "Invalid topic config: all configs to be deleted must be in the format \"key\".");
            }
        });

        final Properties propsToBeDeleted = new Properties();
        Utils.foreach(configsToBeDeleted, new Callable1<String[]>() {
            @Override
            public void apply(String[] pair) {
                propsToBeDeleted.setProperty(pair[0].trim(), "");
            }
        });
        LogConfigs.validateNames(propsToBeDeleted);

        return Utils.mapList(configsToBeDeleted, new Function1<String[], String>() {
            @Override
            public String apply(String[] pair) {
                return pair[0];
            }
        });
    }

    public static Multimap<Integer, Integer> parseReplicaAssignment(String replicaAssignmentList) {
        String[] partitionList = replicaAssignmentList.split(",");
        Multimap<Integer, Integer> ret = HashMultimap.create();
        for (int i = 0; i < partitionList.length; ++i) {
            List<Integer> brokerList = Utils.mapList(partitionList[i].split(":"), new Function1<String, Integer>() {
                @Override
                public Integer apply(String s) {
                    return Integer.parseInt(s.trim());
                }
            });
            ret.putAll(i, brokerList);
            if (ret.get(i).size() != ret.get(0).size())
                throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList);
        }


        return ret;
    }

    static class TopicCommandOptions {
        public String[] args;

        TopicCommandOptions(String[] args) {
            this.args = args;
            init();
        }

        public OptionParser parser;
        public OptionSpec<String> zkConnectOpt;
        public OptionSpec<?> listOpt;
        public OptionSpec<?> createOpt;
        public OptionSpec<?> alterOpt;
        public OptionSpec<?> deleteOpt;
        public OptionSpec<?> describeOpt;
        public OptionSpec<?> helpOpt;
        public OptionSpec<String> topicOpt;
        public OptionSpec<String> configOpt;
        public OptionSpec<String> deleteConfigOpt;
        public OptionSpec<Integer> partitionsOpt;
        public OptionSpec<Integer> replicationFactorOpt;
        public OptionSpec<String> replicaAssignmentOpt;
        public OptionSpec<?> reportUnderReplicatedPartitionsOpt;
        public OptionSpec<?> reportUnavailablePartitionsOpt;
        public OptionSpec<?> topicsWithOverridesOpt;

        public OptionSet options;

        private void init() {

            parser = new OptionParser();
            zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                    "Multiple URLS can be given to allow fail-over.")
                    .withRequiredArg()
                    .describedAs("urls")
                    .ofType(String.class);
            listOpt = parser.accepts("list", "List all available topics.");
            createOpt = parser.accepts("create", "Create a new topic.");
            alterOpt = parser.accepts("alter", "Alter the configuration for the topic.");
            deleteOpt = parser.accepts("delete", "Delete the topic.");
            describeOpt = parser.accepts("describe", "List details for the given topics.");
            helpOpt = parser.accepts("help", "Print usage information.");
            topicOpt = parser.accepts("topic", "The topic to be create, alter, delete, or describe. Can also accept a regular " +
                    "expression except for --create option")
                    .withRequiredArg()
                    .describedAs("topic")
                    .ofType(String.class);
            configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered.")
                    .withRequiredArg()
                    .describedAs("name=value")
                    .ofType(String.class);
            deleteConfigOpt = parser.accepts("deleteConfig", "A topic configuration override to be removed for an existing topic")
                    .withRequiredArg()
                    .describedAs("name")
                    .ofType(String.class);
            partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
                    "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                    .withRequiredArg()
                    .describedAs("# of partitions")
                    .ofType(Integer.class);
            replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                    .withRequiredArg()
                    .describedAs("replication factor")
                    .ofType(Integer.class);
            replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created.")
                    .withRequiredArg()
                    .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                            "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                    .ofType(String.class);
            reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                    "if set when describing topics, only show under replicated partitions");
            reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                    "if set when describing topics, only show partitions whose leader is not available");
            topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                    "if set when listing topics, only show topics that have overridden configs");


            options = parser.parse(args);
        }
    }
}
