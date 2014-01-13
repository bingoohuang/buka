package kafka.server;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.yammer.metrics.core.Gauge;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Callable1;
import kafka.utils.Callable2;
import kafka.utils.Function2;
import kafka.utils.Function3;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public abstract class AbstractFetcherManager extends KafkaMetricsGroup {
    protected String name;
    public String metricPrefix;
    public int numFetchers; /* = 1*/

    public AbstractFetcherManager(String name, String metricPrefix, int numFetchers) {
        this.name = name;
        this.metricPrefix = metricPrefix;
        this.numFetchers = numFetchers;
        init();
    }

    private void init() {
        logger = LoggerFactory.getLogger(AbstractFetcherManager.class + "[" + name + "] ");

        newGauge(
                metricPrefix + "-MaxLag",
                new Gauge<Long>() {
                    // current max lag across all fetchers/topics/partitions
                    @Override
                    public Long value() {
                        return Utils.foldLeft(fetcherThreadMap, 0L, new Function3<Long, BrokerAndFetcherId, AbstractFetcherThread, Long>() {
                            @Override
                            public Long apply(Long curMaxAll, BrokerAndFetcherId arg2, AbstractFetcherThread arg3) {
                                return Math.max(Utils.foldLeft(arg3.fetcherLagStats.stats, 0L, new Function2<Long, Map.Entry<AbstractFetcherThread.ClientIdBrokerTopicPartition, AbstractFetcherThread.FetcherLagMetrics>, Long>() {
                                    @Override
                                    public Long apply(Long curMaxThread, Map.Entry<AbstractFetcherThread.ClientIdBrokerTopicPartition, AbstractFetcherThread.FetcherLagMetrics> fetcherLagStatsEntry) {
                                        return Math.max(curMaxThread, fetcherLagStatsEntry.getValue().lag());
                                    }
                                }), curMaxAll);
                            }
                        });
                    }
                });

        newGauge(
                metricPrefix + "-MinFetchRate",
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        Tuple2<BrokerAndFetcherId, AbstractFetcherThread> head = Utils.head(fetcherThreadMap);
                        Double headRate = head != null ? head._2.fetcherStats.requestRate.oneMinuteRate() : 0;

                        return Utils.foldLeft(fetcherThreadMap, headRate, new Function3<Double, BrokerAndFetcherId, AbstractFetcherThread, Double>() {
                            @Override
                            public Double apply(Double curMinAll, BrokerAndFetcherId arg2, AbstractFetcherThread _2) {
                                return Math.min(_2.fetcherStats.requestRate.oneMinuteRate(), curMinAll);
                            }
                        });
                    }

                });
    }

    // map of (source broker_id, fetcher_id per source broker) => fetcher
    private Map<BrokerAndFetcherId, AbstractFetcherThread> fetcherThreadMap = Maps.newHashMap();
    private Object mapLock = new Object();
    Logger logger;


    private int getFetcherId(String topic, int partitionId) {
        return Utils.abs(31 * topic.hashCode() + partitionId) % numFetchers;
    }

    // to be defined in subclass to create a specific fetcher
    public abstract AbstractFetcherThread createFetcherThread(int fetcherId, Broker sourceBroker);

    public void addFetcherForPartitions(Map<TopicAndPartition, BrokerAndInitialOffset> partitionAndOffsets) {
        synchronized (mapLock) {
            Table<BrokerAndFetcherId, TopicAndPartition, BrokerAndInitialOffset> partitionsPerFetcher = Utils.groupBy(partitionAndOffsets, new Function2<TopicAndPartition, BrokerAndInitialOffset, BrokerAndFetcherId>() {
                @Override
                public BrokerAndFetcherId apply(TopicAndPartition topicAndPartition, BrokerAndInitialOffset brokerAndInitialOffset) {
                    return new BrokerAndFetcherId(brokerAndInitialOffset.broker, getFetcherId(topicAndPartition.topic, topicAndPartition.partition));
                }
            });

            Utils.foreach(partitionsPerFetcher, new Callable2<BrokerAndFetcherId, Map<TopicAndPartition, BrokerAndInitialOffset>>() {
                @Override
                public void apply(BrokerAndFetcherId brokerAndFetcherId, Map<TopicAndPartition, BrokerAndInitialOffset> partitionAndOffsets) {
                    AbstractFetcherThread fetcherThread = fetcherThreadMap.get(brokerAndFetcherId);
                    if (fetcherThread == null) {
                        fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker);
                        fetcherThreadMap.put(brokerAndFetcherId, fetcherThread);
                        fetcherThread.start();
                    }

                    fetcherThreadMap.get(brokerAndFetcherId).addPartitions(Utils.map(partitionAndOffsets, new Function2<TopicAndPartition, BrokerAndInitialOffset, Tuple2<TopicAndPartition, Long>>() {
                        @Override
                        public Tuple2<TopicAndPartition, Long> apply(TopicAndPartition topicAndPartition, BrokerAndInitialOffset brokerAndInitOffset) {
                            return Tuple2.make(topicAndPartition, brokerAndInitOffset.initOffset);
                        }
                    }));
                }
            });

        }

        logger.info("Added fetcher for partitions {}", Utils.mapList(partitionAndOffsets, new Function2<TopicAndPartition, BrokerAndInitialOffset, String>() {
            @Override
            public String apply(TopicAndPartition topicAndPartition, BrokerAndInitialOffset brokerAndInitialOffset) {
                return "[" + topicAndPartition + ", initOffset " + brokerAndInitialOffset.initOffset + " to broker " + brokerAndInitialOffset.broker + "] ";
            }
        }));

    }

    public void removeFetcherForPartitions(final Set<TopicAndPartition> partitions) {
        synchronized (mapLock) {
            Utils.foreach(fetcherThreadMap, new Callable2<BrokerAndFetcherId, AbstractFetcherThread>() {
                @Override
                public void apply(BrokerAndFetcherId key, AbstractFetcherThread fetcher) {
                    fetcher.removePartitions(partitions);
                }
            });
        }
        logger.info("Removed fetcher for partitions {}", partitions);
    }

    public void shutdownIdleFetcherThreads() {
        synchronized (mapLock) {
            final Set<BrokerAndFetcherId> keysToBeRemoved = Sets.newHashSet();
            Utils.foreach(fetcherThreadMap, new Callable2<BrokerAndFetcherId, AbstractFetcherThread>() {
                @Override
                public void apply(BrokerAndFetcherId key, AbstractFetcherThread fetcher) {
                    if (fetcher.partitionCount() <= 0) {
                        fetcher.shutdown();
                        keysToBeRemoved.add(key);
                    }
                }
            });

            Utils.foreach(keysToBeRemoved, new Callable1<BrokerAndFetcherId>() {
                @Override
                public void apply(BrokerAndFetcherId brokerAndFetcherId) {
                    fetcherThreadMap.remove(brokerAndFetcherId);
                }
            });
        }
    }

    public void closeAllFetchers() {
        synchronized (mapLock) {
            Utils.foreach(fetcherThreadMap, new Callable2<BrokerAndFetcherId, AbstractFetcherThread>() {
                @Override
                public void apply(BrokerAndFetcherId _, AbstractFetcherThread fetcher) {
                    fetcher.shutdown();
                }
            });
            fetcherThreadMap.clear();
        }
    }

    public static class BrokerAndFetcherId {
        public Broker broker;
        public int fetcherId;

        public BrokerAndFetcherId(Broker broker, int fetcherId) {
            this.broker = broker;
            this.fetcherId = fetcherId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BrokerAndFetcherId that = (BrokerAndFetcherId) o;

            if (fetcherId != that.fetcherId) return false;
            if (broker != null ? !broker.equals(that.broker) : that.broker != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = broker != null ? broker.hashCode() : 0;
            result = 31 * result + fetcherId;
            return result;
        }

        @Override
        public String toString() {
            return "BrokerAndFetcherId{" +
                    "broker=" + broker +
                    ", fetcherId=" + fetcherId +
                    '}';
        }
    }

    public static class BrokerAndInitialOffset {
        public Broker broker;
        public long initOffset;

        public BrokerAndInitialOffset(Broker broker, long initOffset) {
            this.broker = broker;
            this.initOffset = initOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BrokerAndInitialOffset that = (BrokerAndInitialOffset) o;

            if (initOffset != that.initOffset) return false;
            if (broker != null ? !broker.equals(that.broker) : that.broker != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = broker != null ? broker.hashCode() : 0;
            result = 31 * result + (int) (initOffset ^ (initOffset >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "BrokerAndInitialOffset{" +
                    "broker=" + broker +
                    ", initOffset=" + initOffset +
                    '}';
        }
    }
}
