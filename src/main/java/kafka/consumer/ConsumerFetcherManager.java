package kafka.consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.client.ClientUtils;
import kafka.cluster.Broker;
import kafka.cluster.Cluster;
import kafka.common.TopicAndPartition;
import kafka.server.AbstractFetcherManager;
import kafka.server.AbstractFetcherThread;
import kafka.utils.Callable0;
import kafka.utils.Callable1;
import kafka.utils.Function1;
import kafka.utils.Function2;
import kafka.utils.ShutdownableThread;
import kafka.utils.SystemTime;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static kafka.utils.ZkUtils.getAllBrokersInCluster;

/**
 * Usage:
 * Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 * until shutdown() is called.
 */
public class ConsumerFetcherManager extends AbstractFetcherManager {
    private String consumerIdString;
    private ConsumerConfig config;
    private ZkClient zkClient;

    public ConsumerFetcherManager(String consumerIdString, ConsumerConfig config, ZkClient zkClient) {
        super("ConsumerFetcherManager-" + SystemTime.instance.milliseconds(), config.clientId, 1);
        this.consumerIdString = consumerIdString;
        this.config = config;
        this.zkClient = zkClient;
    }

    private Map<TopicAndPartition, PartitionTopicInfo> partitionMap = null;
    private Cluster cluster = null;
    private Set<TopicAndPartition> noLeaderPartitionSet = Sets.newHashSet();
    private ReentrantLock lock = new ReentrantLock();
    private Condition cond = lock.newCondition();
    private ShutdownableThread leaderFinderThread = null;
    private AtomicInteger correlationId = new AtomicInteger(0);
    Logger logger = LoggerFactory.getLogger(ConsumerFetcherManager.class);


    @Override
    public AbstractFetcherThread createFetcherThread(int fetcherId, Broker sourceBroker) {
        return new ConsumerFetcherThread(
                "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
                config, sourceBroker, partitionMap, this);
    }

    public class LeaderFinderThread extends ShutdownableThread {
        public LeaderFinderThread(String name) {
            super(name);
        }

        // thread responsible for adding the fetcher to the right broker when leader is available
        @Override
        public void doWork() {
            final Map<TopicAndPartition, Broker> leaderForPartitionsMap = Maps.newHashMap();
            lock.lock();
            try {
                while (noLeaderPartitionSet.isEmpty()) {
                    logger.trace("No partition for leader election.");
                    Utils.await(cond);
                }

                logger.trace("Partitions without leader {}", noLeaderPartitionSet);
                List<Broker> brokers = getAllBrokersInCluster(zkClient);
                List<TopicMetadata> topicsMetadata = ClientUtils.fetchTopicMetadata(Utils.mapSet(noLeaderPartitionSet, new Function1<TopicAndPartition, String>() {
                    @Override
                    public String apply(TopicAndPartition m) {
                        return m.topic;
                    }
                }),
                        brokers,
                        config.clientId,
                        config.socketTimeoutMs,
                        correlationId.getAndIncrement()).topicsMetadata;
                if (logger.isDebugEnabled()) {
                    Utils.foreach(topicsMetadata, new Callable1<TopicMetadata>() {
                        @Override
                        public void apply(TopicMetadata topicMetadata) {
                            logger.debug(topicMetadata.toString());
                        }
                    });
                }

                Utils.foreach(topicsMetadata, new Callable1<TopicMetadata>() {
                    @Override
                    public void apply(TopicMetadata tmd) {
                        final String topic = tmd.topic;
                        Utils.foreach(tmd.partitionsMetadata, new Callable1<PartitionMetadata>() {
                            @Override
                            public void apply(PartitionMetadata pmd) {
                                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, pmd.partitionId);
                                if (pmd.leader != null && noLeaderPartitionSet.contains(topicAndPartition)) {
                                    Broker leaderBroker = pmd.leader;
                                    leaderForPartitionsMap.put(topicAndPartition, leaderBroker);
                                    noLeaderPartitionSet.remove(topicAndPartition);
                                }
                            }
                        });
                    }
                });

            } catch (Throwable t) {
                if (!isRunning.get())
                    throw t; /* If this thread is stopped, propagate this exception to kill the thread. */
                else
                    logger.warn("Failed to find leader for {}", noLeaderPartitionSet, t);

            } finally {
                lock.unlock();
            }

            try {
                addFetcherForPartitions(Utils.map(leaderForPartitionsMap, new Function2<TopicAndPartition, Broker, Tuple2<TopicAndPartition, BrokerAndInitialOffset>>() {
                    @Override
                    public Tuple2<TopicAndPartition, BrokerAndInitialOffset> apply(TopicAndPartition topicAndPartition, Broker broker) {
                        return Tuple2.make(topicAndPartition, new BrokerAndInitialOffset(broker, partitionMap.get(topicAndPartition).getFetchOffset()));
                    }
                }));
            } catch (Throwable t) {
                if (!isRunning.get())
                    throw t; /* If this thread is stopped, propagate this exception to kill the thread. */
                else {
                    logger.warn("Failed to add leader for partitions {}; will retry", leaderForPartitionsMap.keySet(), t);
                    lock.lock();
                    noLeaderPartitionSet.addAll(leaderForPartitionsMap.keySet());
                    lock.unlock();
                }
            }

            shutdownIdleFetcherThreads();

            Utils.sleep(config.refreshLeaderBackoffMs);
        }
    }

    public void startConnections(final Iterable<PartitionTopicInfo> topicInfos, final Cluster cluster) {
        leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread");
        leaderFinderThread.start();

        Utils.inLock(lock, new Callable0() {
            @Override
            public void apply() {
                partitionMap = Utils.map(topicInfos, new Function1<PartitionTopicInfo, Tuple2<TopicAndPartition, PartitionTopicInfo>>() {
                    @Override
                    public Tuple2<TopicAndPartition, PartitionTopicInfo> apply(PartitionTopicInfo tpi) {
                        return Tuple2.make(new TopicAndPartition(tpi.topic, tpi.partitionId), tpi);
                    }
                });
                ConsumerFetcherManager.this.cluster = cluster;
                noLeaderPartitionSet.addAll(Utils.mapList(topicInfos, new Function1<PartitionTopicInfo, TopicAndPartition>() {
                    @Override
                    public TopicAndPartition apply(PartitionTopicInfo tpi) {
                        return new TopicAndPartition(tpi.topic, tpi.partitionId);
                    }
                }));
                cond.signalAll();
            }
        });
    }

    public void stopConnections() {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
        logger.info("Stopping leader finder thread");
        if (leaderFinderThread != null) {
            leaderFinderThread.shutdown();
            leaderFinderThread = null;
        }

        logger.info("Stopping all fetchers");
        closeAllFetchers();

        // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
        partitionMap = null;
        noLeaderPartitionSet.clear();

        logger.info("All connections stopped");
    }

    public void addPartitionsWithError(final Iterable<TopicAndPartition> partitionList) {
        logger.debug("adding partitions with error {}", partitionList);
        Utils.inLock(lock, new Callable0() {
            @Override
            public void apply() {
                if (partitionMap != null) {
                    noLeaderPartitionSet.addAll(Lists.newArrayList(partitionList));
                    cond.signalAll();
                }
            }
        });
    }
}
