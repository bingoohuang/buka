package kafka.consumer;


import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.yammer.metrics.core.Gauge;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.ConsumerRebalanceFailedException;
import kafka.common.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaMetricsReporter;
import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.*;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;
import static kafka.utils.ZkUtils.*;

/**
 * This class handles the consumers interaction with zookeeper
 * <p/>
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 * <p/>
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 * <p/>
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 * "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 * "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 * <p/>
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 * <p/>
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 */
public class ZookeeperConsumerConnector extends KafkaMetricsGroup implements ConsumerConnector {
    public static FetchedDataChunk shutdownCommand = new FetchedDataChunk(null, null, -1L);

    public ConsumerConfig config;
    public boolean enableFetcher; // for testing only

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        this.config = config;
        this.enableFetcher = enableFetcher;

        init();
    }

    private void init() {
        String consumerUuid = null;
        if (config.consumerId != null) { // for testing only
            consumerUuid = config.consumerId;
        } else { // generate unique consumerId automatically
            UUID uuid = UUID.randomUUID();
            consumerUuid = String.format("%s-%d-%s", Utils.getHostName(),
                    System.currentTimeMillis(),
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
        }
        consumerIdString = config.groupId + "_" + consumerUuid;

        logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class + "[" + consumerIdString + "]");

        connectZk();
        createFetcher();
        if (config.autoCommitEnable) {
            scheduler.startup();
            logger.info("starting auto committer every {} ms", config.autoCommitIntervalMs);
            scheduler.schedule("kafka-consumer-autocommit", new Runnable() {
                @Override
                public void run() {
                    autoCommit();
                }
            },
                    /*delay =*/ config.autoCommitIntervalMs,
                    /*period =*/ config.autoCommitIntervalMs,
                    /*unit =*/ TimeUnit.MILLISECONDS);
        }

        KafkaMetricsReporter.startReporters(config.props);
    }

    Logger logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);

    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private ConsumerFetcherManager fetcher = null;
    private ZkClient zkClient = null;
    private Pool<String, Pool<Integer, PartitionTopicInfo>> topicRegistry = new Pool<String, Pool<Integer, PartitionTopicInfo>>();
    private Pool<TopicAndPartition, Long> checkpointedOffsets = new Pool<TopicAndPartition, Long>();
    private Pool<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> topicThreadIdAndQueues = new Pool<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>>();
    private KafkaScheduler scheduler = new KafkaScheduler(1, /*threadNamePrefix = */"kafka-consumer-scheduler-");
    private AtomicBoolean messageStreamCreated = new AtomicBoolean(false);

    private ZKSessionExpireListener sessionExpirationListener = null;
    private ZKTopicPartitionChangeListener topicPartitionChangeListener = null;
    private ZKRebalancerListener loadBalancerListener = null;

    private ZookeeperTopicEventWatcher wildcardTopicWatcher = null;

    public String consumerIdString;


    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }


    @Override
    public Multimap<String, KafkaStream<byte[], byte[]>> createMessageStreams(Map<String, Integer> topicCountMap) {
        return createMessageStreams(topicCountMap, new DefaultDecoder(), new DefaultDecoder());
    }

    @Override
    public <K, V> Multimap<String, KafkaStream<K, V>> createMessageStreams(Map<String, Integer> topicCountMap, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        if (messageStreamCreated.getAndSet(true))
            throw new RuntimeException(this.getClass().getSimpleName() +
                    " can create message streams at most once");
        return consume(topicCountMap, keyDecoder, valueDecoder);
    }


    @Override
    public <K, V> List<KafkaStream<K, V>> createMessageStreamsByFilter(TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
        WildcardStreamsHandler<K, V> wildcardStreamsHandler = new WildcardStreamsHandler<K, V>(topicFilter, numStreams, keyDecoder, valueDecoder);
        return wildcardStreamsHandler.streams();
    }

    private void createFetcher() {
        if (enableFetcher)
            fetcher = new ConsumerFetcherManager(consumerIdString, config, zkClient);
    }

    private void connectZk() {
        logger.info("Connecting to zookeeper instance at {}", config.zkConnect);
        zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer.instance);
    }

    @Override
    public void shutdown() {
        synchronized (rebalanceLock) {
            boolean canShutdown = isShuttingDown.compareAndSet(false, true);
            if (canShutdown) {
                logger.info("ZKConsumerConnector shutting down");

                if (wildcardTopicWatcher != null)
                    wildcardTopicWatcher.shutdown();
                try {
                    if (config.autoCommitEnable)
                        scheduler.shutdown();
                    if (fetcher != null) {
                        fetcher.stopConnections();
                    }
                    sendShutdownToAllQueues();
                    if (config.autoCommitEnable)
                        commitOffsets();
                    if (zkClient != null) {
                        zkClient.close();
                        zkClient = null;
                    }
                } catch (Throwable e) {
                    logger.error("error during consumer connector shutdown", e);
                }
                logger.info("ZKConsumerConnector shut down completed");
            }
        }
    }

    public <K, V> Multimap<String, KafkaStream<K, V>> consume(Map<String, Integer> topicCountMap, final Decoder<K> keyDecoder, final Decoder<V> valueDecoder) {
        logger.debug("entering consume ");
        if (topicCountMap == null)
            throw new RuntimeException("topicCountMap is null");

        TopicCount topicCount = TopicCounts.constructTopicCount(consumerIdString, topicCountMap);

        Map<String, Set<String>> topicThreadIds = topicCount.getConsumerThreadIdsPerTopic();

        // make a list of (queue,stream) pairs, one pair for each threadId
        final List<Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> queuesAndStreams = Lists.newArrayList();
        Utils.foreach(topicThreadIds.values(), new Callable1<Set<String>>() {
            @Override
            public void apply(Set<String> threadIdSet) {
                Utils.foreach(threadIdSet, new Callable1<String>() {
                    @Override
                    public void apply(String _) {
                        LinkedBlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                        KafkaStream<K, V> stream = new KafkaStream<K, V>(
                                queue, config.consumerTimeoutMs, keyDecoder, valueDecoder, config.clientId);
                        queuesAndStreams.add(Tuple2.make(queue, stream));
                    }
                });
            }
        });


        ZKGroupDirs dirs = new ZKGroupDirs(config.groupId);
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        reinitializeConsumer(topicCount, queuesAndStreams);

        return loadBalancerListener.kafkaMessageAndMetadataStreams;
    }

    private <K, V> void reinitializeConsumer(TopicCount topicCount,
                                             final List<Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> queuesAndStreams) {
        ZKGroupDirs dirs = new ZKGroupDirs(config.groupId);

        // listener to consumer and partition changes
        if (loadBalancerListener == null) {
            Multimap<String, KafkaStream<K, V>> topicStreamsMap = HashMultimap.create();
            loadBalancerListener = new ZKRebalancerListener(
                    config.groupId, consumerIdString, topicStreamsMap);
        }

        // create listener for session expired event if not exist yet
        if (sessionExpirationListener == null)
            sessionExpirationListener = new ZKSessionExpireListener(
                    dirs, consumerIdString, topicCount, loadBalancerListener);

        // create listener for topic partition change event if not exist yet
        if (topicPartitionChangeListener == null)
            topicPartitionChangeListener = new ZKTopicPartitionChangeListener(loadBalancerListener);

        final Multimap<String, KafkaStream<K, V>> topicStreamsMap = loadBalancerListener.kafkaMessageAndMetadataStreams;

        // map of {topic -> Set(thread-1, thread-2, ...)}
        Map<String, Set<String>> consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic();

        List<Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> allQueuesAndStreams;
        if (topicCount instanceof WildcardTopicCount) {
             /*
              * Wild-card consumption streams share the same queues, so we need to
              * duplicate the list for the subsequent zip operation.
              */
            allQueuesAndStreams = Utils.flatList(0, consumerThreadIdsPerTopic.keySet().size(), new Function1<Integer, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>() {
                @Override
                public Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>> apply(Integer _) {
                    return queuesAndStreams.get(_);
                }
            });

        } else/* if (topicCount instanceof StaticTopicCount)*/ {
            allQueuesAndStreams = queuesAndStreams;
        }

        final List<Tuple2<String, String>> topicThreadIds = Lists.newArrayList();

        Utils.foreach(consumerThreadIdsPerTopic, new Callable2<String, Set<String>>() {
            @Override
            public void apply(final String topic, Set<String> threadIds) {
                Utils.foreach(threadIds, new Callable1<String>() {
                    @Override
                    public void apply(String _) {
                        topicThreadIds.add(Tuple2.make(topic, _));
                    }
                });
            }
        });

        checkState(topicThreadIds.size() == allQueuesAndStreams.size(),
                String.format("Mismatch between thread ID count (%d) and queue count (%d)",
                        topicThreadIds.size(), allQueuesAndStreams.size()));
        List<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>>
                threadQueueStreamPairs = Utils.zip(topicThreadIds, allQueuesAndStreams);

        Utils.foreach(threadQueueStreamPairs, new Callable1<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>>() {
            @Override
            public void apply(Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> e) {
                Tuple2<String, String> topicThreadId = e._1;
                final LinkedBlockingQueue<FetchedDataChunk> q = e._2._1;
                topicThreadIdAndQueues.put(topicThreadId, q);
                logger.debug("Adding topicThreadId {} and queue {} to topicThreadIdAndQueues data structure", topicThreadId, q);
                newGauge(
                        config.clientId + "-" + config.groupId + "-" + topicThreadId._1 + "-" + topicThreadId._2 + "-FetchQueueSize",
                        new Gauge<Integer>() {
                            @Override
                            public Integer value() {
                                return q.size();
                            }
                        });

            }
        });

        Multimap<String, Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>>
                groupedByTopic = Utils.groupby(threadQueueStreamPairs, new Function1<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>, String>() {
            @Override
            public String apply(Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> _) {
                return _._1._1;
            }
        });

        Utils.foreach(groupedByTopic, new Callable2<String, Collection<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>>>() {
            @Override
            public void apply(String topic, Collection<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>> _2) {
                List<KafkaStream<K, V>> streams = Utils.mapList(_2, new Function1<Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>, KafkaStream<K, V>>() {
                    @Override
                    public KafkaStream<K, V> apply(Tuple2<Tuple2<String, String>, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> _) {
                        return _._2._2;
                    }
                });

                topicStreamsMap.putAll(topic, streams);
                logger.debug("adding topic {} and {} streams to map.", topic, streams.size());
            }
        });

        // listener to consumer and partition changes
        zkClient.subscribeStateChanges(sessionExpirationListener);

        zkClient.subscribeChildChanges(dirs.consumerRegistryDir(), loadBalancerListener);

        Utils.foreach(topicStreamsMap, new Callable2<String, Collection<KafkaStream<K, V>>>() {
            @Override
            public void apply(String _1, Collection<KafkaStream<K, V>> kafkaStreams) {
                // register on broker partition path changes
                String topicPath = BrokerTopicsPath + "/" + _1;
                zkClient.subscribeDataChanges(topicPath, topicPartitionChangeListener);
            }
        });

        // explicitly trigger load balancing for this consumer
        loadBalancerListener.syncedRebalance();
    }

    // this API is used by unit tests only
    public Pool<String, Pool<Integer, PartitionTopicInfo>> getTopicRegistry() {
        return topicRegistry;
    }

    private void registerConsumerInZK(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        logger.info("begin registering consumer {} in ZK", consumerIdString);
        String timestamp = SystemTime.instance.milliseconds() + "";
        String consumerRegistrationInfo = Json.encode(ImmutableMap.of("version", 1, "subscription", topicCount.getTopicCountMap(), "pattern", topicCount.pattern(),
                "timestamp", timestamp));

        createEphemeralPathExpectConflictHandleZKBug(zkClient, dirs.consumerRegistryDir() + "/" + consumerIdString, consumerRegistrationInfo, null,
                new Function2<String, Object, Boolean>() {
                    @Override
                    public Boolean apply(String consumerZKString, Object consumer) {
                        return true;
                    }
                }, config.zkSessionTimeoutMs);
        logger.info("end registering consumer {} in ZK", consumerIdString);
    }

    private void sendShutdownToAllQueues() {
        Utils.foreach(topicThreadIdAndQueues.values(), new Callable1<BlockingQueue<FetchedDataChunk>>() {
            @Override
            public void apply(BlockingQueue<FetchedDataChunk> queue) {
                logger.debug("Clearing up queue");
                queue.clear();
                Utils.put(queue, ZookeeperConsumerConnector.shutdownCommand);
                logger.debug("Cleared queue and sent shutdown command");
            }
        });
    }

    public void autoCommit() {
        logger.trace("auto committing");
        try {
            commitOffsets();
        } catch (Throwable t) {
            // log it and let it go
            logger.error("exception during autoCommit: ", t);
        }
    }

    @Override
    public void commitOffsets() {
        if (zkClient == null) {
            logger.error("zk client is null. Cannot commit offsets");
            return;
        }

        Utils.foreach(topicRegistry, new Callable1<Map.Entry<String, Pool<Integer, PartitionTopicInfo>>>() {
            @Override
            public void apply(Map.Entry<String, Pool<Integer, PartitionTopicInfo>> _) {
                final String topic = _.getKey();
                Pool<Integer, PartitionTopicInfo> infos = _.getValue();
                final ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId, topic);
                Utils.foreach(infos.values(), new Callable1<PartitionTopicInfo>() {
                    @Override
                    public void apply(PartitionTopicInfo info) {
                        long newOffset = info.getConsumeOffset();
                        if (newOffset != checkpointedOffsets.get(new TopicAndPartition(topic, info.partitionId))) {
                            try {
                                updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + info.partitionId, newOffset + "");
                                checkpointedOffsets.put(new TopicAndPartition(topic, info.partitionId), newOffset);
                            } catch (Throwable t) {
                                // log it and let it go
                                logger.warn("exception during commitOffsets", t);
                            }
                            logger.debug("Committed offset {} for topic {}", newOffset, info);
                        }
                    }
                });
            }
        });
    }

    class ZKSessionExpireListener implements IZkStateListener {
        ZKGroupDirs dirs;
        String consumerIdString;
        TopicCount topicCount;
        ZKRebalancerListener loadBalancerListener;

        ZKSessionExpireListener(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount, ZKRebalancerListener loadBalancerListener) {
            this.dirs = dirs;
            this.consumerIdString = consumerIdString;
            this.topicCount = topicCount;
            this.loadBalancerListener = loadBalancerListener;
        }

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
            /**
             *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
             *  connection for us. We need to release the ownership of the current consumer and re-register this
             *  consumer in the consumer registry and trigger a rebalance.
             */
            logger.info("ZK expired; release old broker parition ownership; re-register consumer ", consumerIdString);
            loadBalancerListener.resetState();
            registerConsumerInZK(dirs, consumerIdString, topicCount);
            // explicitly trigger load balancing for this consumer
            loadBalancerListener.syncedRebalance();
            // There is no need to resubscribe to child and state changes.
            // The child change watchers will be set inside rebalance when we read the children list.
        }

        @Override
        public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

        }
    }

    class ZKTopicPartitionChangeListener implements IZkDataListener {
        ZKRebalancerListener loadBalancerListener;

        ZKTopicPartitionChangeListener(ZKRebalancerListener loadBalancerListener) {
            this.loadBalancerListener = loadBalancerListener;
        }

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            try {
                logger.info("Topic info for path {} changed to {}, triggering rebalance", dataPath, data);
                // queue up the rebalance event
                loadBalancerListener.rebalanceEventTriggered();
                // There is no need to re-subscribe the watcher since it will be automatically
                // re-registered upon firing of this event by zkClient
            } catch (Throwable e) {
                logger.error("Error while handling topic partition change for data path {}", dataPath, e);
            }
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            // TODO: This need to be implemented when we support delete topic
            logger.warn("Topic for path {} gets deleted, which should not happen at this time", dataPath);
        }
    }

    class ZKRebalancerListener<K, V> implements IZkChildListener {
        public String group;
        public String consumerIdString;
        public Multimap<String, KafkaStream<K, V>> kafkaMessageAndMetadataStreams;

        ZKRebalancerListener(String group, final String consumerIdString, Multimap<String, KafkaStream<K, V>> kafkaMessageAndMetadataStreams) {
            this.group = group;
            this.consumerIdString = consumerIdString;
            this.kafkaMessageAndMetadataStreams = kafkaMessageAndMetadataStreams;

            watcherExecutorThread = new Thread(consumerIdString + "_watcher_executor") {
                @Override
                public void run() {
                    logger.info("starting watcher executor thread for consumer {}", consumerIdString);
                    boolean doRebalance = false;
                    while (!isShuttingDown.get()) {
                        try {
                            lock.lock();
                            try {
                                if (!isWatcherTriggered)
                                    Utils.await(cond, 1000, TimeUnit.MILLISECONDS); // wake up periodically so that it can check the shutdown flag
                            } finally {
                                doRebalance = isWatcherTriggered;
                                isWatcherTriggered = false;
                                lock.unlock();
                            }
                            if (doRebalance)
                                syncedRebalance();
                        } catch (Throwable t) {
                            logger.error("error during syncedRebalance", t);
                        }
                    }
                    logger.info("stopping watcher executor thread for consumer {}", consumerIdString);
                }
            };
            watcherExecutorThread.start();
        }

        private boolean isWatcherTriggered = false;
        private ReentrantLock lock = new ReentrantLock();
        private Condition cond = lock.newCondition();
        private Thread watcherExecutorThread;

        @Override
        public void handleChildChange(String s, List<String> strings) throws Exception {
            rebalanceEventTriggered();
        }

        public void rebalanceEventTriggered() {
            Utils.inLock(lock, new Callable0() {
                @Override
                public void apply() {
                    isWatcherTriggered = true;
                    cond.signalAll();
                }
            });
        }

        private void deletePartitionOwnershipFromZK(String topic, int partition) {
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
            String znode = topicDirs.consumerOwnerDir() + "/" + partition;
            deletePath(zkClient, znode);
            logger.debug("Consumer {} releasing {}", consumerIdString, znode);
        }

        private void releasePartitionOwnership(final Pool<String, Pool<Integer, PartitionTopicInfo>> localTopicRegistry) {
            logger.info("Releasing partition ownership");
            Utils.foreach(localTopicRegistry, new Callable1<Map.Entry<String, Pool<Integer, PartitionTopicInfo>>>() {
                @Override
                public void apply(Map.Entry<String, Pool<Integer, PartitionTopicInfo>> _) {
                    final String topic = _.getKey();
                    Pool<Integer, PartitionTopicInfo> infos = _.getValue();
                    Utils.foreach(infos.keys(), new Callable1<Integer>() {
                        @Override
                        public void apply(Integer partition) {
                            deletePartitionOwnershipFromZK(topic, partition);
                        }
                    });
                    localTopicRegistry.remove(topic);
                }
            });
        }

        public void resetState() {
            topicRegistry.clear();
        }

        public void syncedRebalance() {
            synchronized (rebalanceLock) {
                if (isShuttingDown.get()) {
                    return;
                }

                for (int i = 0, ii = config.rebalanceMaxRetries; i < ii; ++i) {
                    logger.info("begin rebalancing consumer {} try #{}", consumerIdString, i);
                    boolean done = false;
                    Cluster cluster = null;
                    try {
                        cluster = getCluster(zkClient);
                        done = rebalance(cluster);
                    } catch (Throwable e) {
                        /** occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
                         * For example, a ZK node can disappear between the time we get all children and the time we try to get
                         * the value of a child. Just let this go since another rebalance will be triggered.
                         **/
                        logger.info("exception during rebalance ", e);
                    }
                    logger.info("end rebalancing consumer " + consumerIdString + " try #" + i);
                    if (done) {
                        return;
                    } else {
              /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
               * clear the cache */
                        logger.info("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                    }
                    // stop all fetchers and clear all the queues to avoid data duplication
                    closeFetchersForQueues(cluster, kafkaMessageAndMetadataStreams, Utils.mapList(topicThreadIdAndQueues, new Function1<Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>>, BlockingQueue<FetchedDataChunk>>() {
                        @Override
                        public BlockingQueue<FetchedDataChunk> apply(Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> q) {
                            return q.getValue();
                        }
                    }));
                    Utils.sleep(config.rebalanceBackoffMs);
                }
            }

            throw new ConsumerRebalanceFailedException(consumerIdString + " can't rebalance after " + config.rebalanceMaxRetries + " retries");
        }


        private boolean rebalance(final Cluster cluster) {
            final Map<String, Set<String>> myTopicThreadIdsMap = TopicCounts.constructTopicCount(group, consumerIdString, zkClient).getConsumerThreadIdsPerTopic();
            final Multimap<String, String> consumersPerTopicMap = getConsumersPerTopic(zkClient, group);
            List<Broker> brokers = getAllBrokersInCluster(zkClient);
            if (brokers.size() == 0) {
                // This can happen in a rare case when there are no brokers available in the cluster when the consumer is started.
                // We log an warning and register for child changes on brokers/id so that rebalance can be triggered when the brokers
                // are up.
                logger.warn("no brokers found when trying to rebalance.");
                zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, loadBalancerListener);
                return true;
            } else {
                Map<String, Multimap<Integer, Integer>> partitionsAssignmentPerTopicMap = getPartitionAssignmentForTopics(zkClient, Lists.newArrayList(myTopicThreadIdsMap.keySet()));
                final Map<String, List<Integer>> partitionsPerTopicMap = Maps.newHashMap();
                Utils.foreach(partitionsAssignmentPerTopicMap, new Callable2<String, Multimap<Integer, Integer>>() {
                    @Override
                    public void apply(String _1, Multimap<Integer, Integer> _2) {
                        ArrayList<Integer> partitions = Lists.newArrayList(_2.keySet());
                        Collections.sort(partitions);
                        partitionsPerTopicMap.put(_1, partitions);
                    }
                });

                /**
                 * fetchers must be stopped to avoid data duplication, since if the current
                 * rebalancing attempt fails, the partitions that are released could be owned by another consumer.
                 * But if we don't stop the fetchers first, this consumer would continue returning data for released
                 * partitions in parallel. So, not stopping the fetchers leads to duplicate data.
                 */
                closeFetchers(cluster, kafkaMessageAndMetadataStreams, myTopicThreadIdsMap);

                releasePartitionOwnership(topicRegistry);

                final Map<Tuple2<String, Integer>, String> partitionOwnershipDecision = Maps.newHashMap();
                final Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry = new Pool<String, Pool<Integer, PartitionTopicInfo>>();

                Utils.foreach(myTopicThreadIdsMap, new Callable2<String, Set<String>>() {
                    @Override
                    public void apply(final String topic, final Set<String> consumerThreadIdSet) {


                        currentTopicRegistry.put(topic, new Pool<Integer, PartitionTopicInfo>());

                        final ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
                        final List<String> curConsumers = Lists.newArrayList(consumersPerTopicMap.get(topic));
                        final List<Integer> curPartitions = partitionsPerTopicMap.get(topic);

                        final int nPartsPerConsumer = curPartitions.size() / curConsumers.size();
                        final int nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

                        logger.info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions +
                                " for topic " + topic + " with consumers: " + curConsumers);

                        Utils.foreach(consumerThreadIdSet, new Callable1<String>() {
                            @Override
                            public void apply(String consumerThreadId) {


                                int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                                assert (myConsumerPosition >= 0);
                                int startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition, nConsumersWithExtraPart);
                                int nParts = nPartsPerConsumer + ((myConsumerPosition + 1 > nConsumersWithExtraPart) ? 0 : 1);

                                /**
                                 *   Range-partition the sorted partitions to consumers for better locality.
                                 *  The first few consumers pick up an extra partition, if any.
                                 */
                                if (nParts <= 0)
                                    logger.warn("No broker partitions consumed by consumer thread {} for topic {}", consumerThreadId, topic);
                                else {
                                    for (int i = startPart; i < startPart + nParts; ++i) {
                                        int partition = curPartitions.get(i);
                                        logger.info("{} attempting to claim partition {}", consumerThreadId, partition);
                                        addPartitionTopicInfo(currentTopicRegistry, topicDirs, partition, topic, consumerThreadId);
                                        // record the partition ownership decision
                                        partitionOwnershipDecision.put(Tuple2.make(topic, partition), consumerThreadId);
                                    }
                                }
                            }
                        });
                    }

                });

                /**
                 * move the partition ownership here, since that can be used to indicate a truly successful rebalancing attempt
                 * A rebalancing attempt is completed successfully only after the fetchers have been started correctly
                 */
                if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                    logger.info("Updating the cache");
                    logger.debug("Partitions per topic cache {}", partitionsPerTopicMap);
                    logger.debug("Consumers per topic cache {}", consumersPerTopicMap);
                    topicRegistry = currentTopicRegistry;
                    updateFetcher(cluster);
                    return true;
                } else {
                    return false;
                }
            }
        }

        private void closeFetchersForQueues(Cluster cluster,
                                            Multimap<String, KafkaStream<K, V>> messageStreams,
                                            Iterable<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
            List<PartitionTopicInfo> allPartitionInfos = Utils.mapLists(topicRegistry.values(), new Function1<Pool<Integer, PartitionTopicInfo>, Collection<PartitionTopicInfo>>() {
                @Override
                public Collection<PartitionTopicInfo> apply(Pool<Integer, PartitionTopicInfo> p) {
                    return p.values();
                }
            });

            if (fetcher != null) {
                fetcher.stopConnections();
                clearFetcherQueues(allPartitionInfos, cluster, queuesToBeCleared, messageStreams);
                logger.info("Committing all offsets after clearing the fetcher queues");
                /**
                 * here, we need to commit offsets before stopping the consumer from returning any more messages
                 * from the current data chunk. Since partition ownership is not yet released, this commit offsets
                 * call will ensure that the offsets committed now will be used by the next consumer thread owning the partition
                 * for the current data chunk. Since the fetchers are already shutdown and this is the last chunk to be iterated
                 * by the consumer, there will be no more messages returned by this iterator until the rebalancing finishes
                 * successfully and the fetchers restart to fetch more data chunks
                 **/
                if (config.autoCommitEnable)
                    commitOffsets();
            }
        }

        private void clearFetcherQueues(Iterable<PartitionTopicInfo> topicInfos, Cluster cluster,
                                        Iterable<BlockingQueue<FetchedDataChunk>> queuesTobeCleared,
                                        Multimap<String, KafkaStream<K, V>> messageStreams) {

            // Clear all but the currently iterated upon chunk in the consumer thread's queue
            Utils.foreach(queuesTobeCleared, new Callable1<BlockingQueue<FetchedDataChunk>>() {
                @Override
                public void apply(BlockingQueue<FetchedDataChunk> _) {
                    _.clear();
                }
            });
            logger.info("Cleared all relevant queues for this fetcher");

            // Also clear the currently iterated upon chunk in the consumer threads
            if (messageStreams != null)
                Utils.foreach(messageStreams, new Callable2<String, Collection<KafkaStream<K, V>>>() {
                    @Override
                    public void apply(String s, Collection<KafkaStream<K, V>> _2) {
                        Utils.foreach(_2, new Callable1<KafkaStream<K, V>>() {
                            @Override
                            public void apply(KafkaStream<K, V> kvKafkaStream) {
                                kvKafkaStream.clear();
                            }
                        });
                    }
                });

            logger.info("Cleared the data chunks in all the consumer message iterators");
        }

        private void closeFetchers(Cluster cluster, Multimap<String, KafkaStream<K, V>> messageStreams,
                                   final Map<String, Set<String>> relevantTopicThreadIdsMap) {
            // only clear the fetcher queues for certain topic partitions that *might* no longer be served by this consumer
            // after this rebalancing attempt
            List<BlockingQueue<FetchedDataChunk>> queuesTobeCleared = Utils.mapList(Utils.filter(topicThreadIdAndQueues, new Predicate<Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>>>() {
                @Override
                public boolean apply(Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> q) {
                    return relevantTopicThreadIdsMap.containsKey(q.getKey()._1);
                }
            }), new Function1<Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>>, BlockingQueue<FetchedDataChunk>>() {
                @Override
                public BlockingQueue<FetchedDataChunk> apply(Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> q) {
                    return q.getValue();
                }
            });

            closeFetchersForQueues(cluster, messageStreams, queuesTobeCleared);
        }

        private void updateFetcher(Cluster cluster) {
            // update partitions for fetcher
            final List<PartitionTopicInfo> allPartitionInfos = Lists.newArrayList();
            Utils.foreach(topicRegistry.values(), new Callable1<Pool<Integer, PartitionTopicInfo>>() {
                @Override
                public void apply(Pool<Integer, PartitionTopicInfo> partitionInfos) {
                    Utils.foreach(partitionInfos.values(), new Callable1<PartitionTopicInfo>() {
                        @Override
                        public void apply(PartitionTopicInfo partition) {
                            allPartitionInfos.add(partition);
                        }
                    });
                }
            });

            logger.info("Consumer {} selected partitions : {}", consumerIdString,
                    Utils.sortWith(allPartitionInfos, new Comparator<PartitionTopicInfo>() {
                        @Override
                        public int compare(PartitionTopicInfo s, PartitionTopicInfo t) {
                            return s.partitionId < t.partitionId ? -1 : (s.partitionId == t.partitionId ? 0 : 1);
                        }
                    }));

            if (fetcher != null)
                fetcher.startConnections(allPartitionInfos, cluster);
        }

        private boolean reflectPartitionOwnershipDecision(Map<Tuple2<String, Integer>, String> partitionOwnershipDecision) {
            final List<Tuple2<String, Integer>> successfullyOwnedPartitions = Lists.newArrayList();
            List<Boolean> partitionOwnershipSuccessful = Utils.mapList(partitionOwnershipDecision, new Function2<Tuple2<String, Integer>, String, Boolean>() {
                @Override
                public Boolean apply(Tuple2<String, Integer> _1, String consumerThreadId) {
                    String topic = _1._1;
                    Integer partition = _1._2;
                    String partitionOwnerPath = getConsumerPartitionOwnerPath(group, topic, partition);
                    try {
                        createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                        logger.info("{} successfully owned partition {} for topic {}", consumerThreadId, partition, topic);
                        successfullyOwnedPartitions.add(Tuple2.make(topic, partition));
                        return true;
                    } catch (ZkNodeExistsException e) {
                        // The node hasn't been deleted by the original owner. So wait a bit and retry.
                        logger.info("waiting for the partition ownership to be deleted: {}", partition);
                        return false;
                    } catch (Throwable e2) {
                        throw e2;
                    }
                }
            });

            int hasPartitionOwnershipFailed = Utils.foldLeft(partitionOwnershipSuccessful, 0, new Function2<Integer, Boolean, Integer>() {
                @Override
                public Integer apply(Integer sum, Boolean decision) {
                    return sum + (decision ? 0 : 1);
                }
            });
      /* even if one of the partition ownership attempt has failed, return false */
            if (hasPartitionOwnershipFailed > 0) {
                // remove all paths that we have owned in ZK
                Utils.foreach(successfullyOwnedPartitions, new Callable1<Tuple2<String, Integer>>() {
                    @Override
                    public void apply(Tuple2<String, Integer> topicAndPartition) {
                        deletePartitionOwnershipFromZK(topicAndPartition._1, topicAndPartition._2);
                    }
                });
                return false;
            } else return true;
        }

        private void addPartitionTopicInfo(Pool<String, Pool<Integer, PartitionTopicInfo>> currentTopicRegistry,
                                           ZKGroupTopicDirs topicDirs, int partition,
                                           String topic, String consumerThreadId) {
            Pool<Integer, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.get(topic);

            String znode = topicDirs.consumerOffsetDir() + "/" + partition;
            String offsetString = readDataMaybeNull(zkClient, znode)._1;
            // If first time starting a consumer, set the initial offset to -1
            long offset = offsetString != null ? Long.parseLong(offsetString) : PartitionTopicInfo.InvalidOffset;

            BlockingQueue<FetchedDataChunk> queue = topicThreadIdAndQueues.get(Tuple2.make(topic, consumerThreadId));
            AtomicLong consumedOffset = new AtomicLong(offset);
            AtomicLong fetchedOffset = new AtomicLong(offset);
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,
                    partition,
                    queue,
                    consumedOffset,
                    fetchedOffset,
                    new AtomicInteger(config.fetchMessageMaxBytes),
                    config.clientId);
            partTopicInfoMap.put(partition, partTopicInfo);
            logger.debug("{} selected new offset {}", partTopicInfo, offset);
            checkpointedOffsets.put(new TopicAndPartition(topic, partition), offset);
        }
    }


    class WildcardStreamsHandler<K, V> implements TopicEventHandler<String> {
        TopicFilter topicFilter;
        int numStreams;
        Decoder<K> keyDecoder;
        Decoder<V> valueDecoder;

        List<Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>> wildcardQueuesAndStreams;
        // bootstrap with existing topics
        private List<String> wildcardTopics;
        private TopicCount wildcardTopicCount;
        ZKGroupDirs dirs;

        WildcardStreamsHandler(final TopicFilter topicFilter, int numStreams, final Decoder<K> keyDecoder, final Decoder<V> valueDecoder) {
            this.topicFilter = topicFilter;
            this.numStreams = numStreams;
            this.keyDecoder = keyDecoder;
            this.valueDecoder = valueDecoder;

            if (messageStreamCreated.getAndSet(true))
                throw new RuntimeException("Each consumer connector can create " +
                        "message streams by filter at most once.");

            wildcardQueuesAndStreams = Utils.flatList(1, numStreams, new Function1<Integer, Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>>() {
                @Override
                public Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>> apply(Integer arg) {
                    LinkedBlockingQueue<FetchedDataChunk> queue = new LinkedBlockingQueue<FetchedDataChunk>(config.queuedMaxMessages);
                    KafkaStream<K, V> stream = new KafkaStream<K, V>(queue,
                            config.consumerTimeoutMs,
                            keyDecoder,
                            valueDecoder,
                            config.clientId);
                    return Tuple2.make(queue, stream);
                }
            });

            wildcardTopics =
                    Utils.filter(getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath), new Predicate<String>() {
                        @Override
                        public boolean apply(String _) {
                            return topicFilter.isTopicAllowed(_);
                        }
                    });

            wildcardTopicCount = TopicCounts.constructTopicCount(
                    consumerIdString, topicFilter, numStreams, zkClient);

            dirs = new ZKGroupDirs(config.groupId);
            registerConsumerInZK(dirs, consumerIdString, wildcardTopicCount);
            reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);

    /*
     * Topic events will trigger subsequent synced rebalances.
     */
            logger.info("Creating topic event watcher for topics {}", topicFilter);
            wildcardTopicWatcher = new ZookeeperTopicEventWatcher(zkClient, this);
        }

        @Override
        public void handleTopicEvent(Collection<String> allTopics) {
            logger.debug("Handling topic event");

            final List<String> updatedTopics = Utils.filter(allTopics, new Predicate<String>() {
                @Override
                public boolean apply(String _) {
                    return topicFilter.isTopicAllowed(_);
                }
            });

            List<String> addedTopics = Utils.filter(updatedTopics, new Predicate<String>() {
                @Override
                public boolean apply(String _) {
                    return !wildcardTopics.contains(_);
                }
            });

            if (!addedTopics.isEmpty())
                logger.info("Topic event: added topics = {}", addedTopics);

      /*
       * TODO: Deleted topics are interesting (and will not be a concern until
       * 0.8 release). We may need to remove these topics from the rebalance
       * listener's map in reinitializeConsumer.
       */
            List<String> deletedTopics = Utils.filter(wildcardTopics, new Predicate<String>() {
                @Override
                public boolean apply(String _) {
                    return !updatedTopics.contains(_);
                }
            });
            if (!deletedTopics.isEmpty())
                logger.info("Topic event: deleted topics = {}", deletedTopics);

            wildcardTopics = updatedTopics;
            logger.info("Topics to consume = {}", wildcardTopics);

            if (!addedTopics.isEmpty() || !deletedTopics.isEmpty())
                reinitializeConsumer(wildcardTopicCount, wildcardQueuesAndStreams);
        }

        public List<KafkaStream<K, V>> streams() {
            return Utils.mapList(wildcardQueuesAndStreams, new Function1<Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>>, KafkaStream<K, V>>() {
                @Override
                public KafkaStream<K, V> apply(Tuple2<LinkedBlockingQueue<FetchedDataChunk>, KafkaStream<K, V>> arg) {
                    return arg._2;
                }
            });
        }
    }
}
