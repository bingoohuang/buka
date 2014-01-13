package kafka.server;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchResponse;
import kafka.api.FetchResponsePartitionData;
import kafka.cluster.Broker;
import kafka.common.ClientIdAndBroker;
import kafka.common.ErrorMapping;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.consumer.PartitionTopicInfo;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.InvalidMessageException;
import kafka.message.MessageAndOffset;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Callable1;
import kafka.utils.Callable2;
import kafka.utils.Function0;
import kafka.utils.Pool;
import kafka.utils.ShutdownableThread;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract class for fetching data from multiple partitions from the same broker.
 */
public abstract class AbstractFetcherThread extends ShutdownableThread {
    public String name, clientId;
    public Broker sourceBroker;
    public int socketTimeout, socketBufferSize, fetchSize, fetcherBrokerId/* = -1*/, maxWait/* = 0*/, minBytes/* = 1*/;
    public boolean isInterruptible/* = true*/;

    protected AbstractFetcherThread(String name, String clientId, Broker sourceBroker, int socketTimeout, int socketBufferSize, int fetchSize, int fetcherBrokerId, int maxWait, int minBytes, boolean isInterruptible) {
        super(name, isInterruptible);
        this.name = name;
        this.clientId = clientId;
        this.sourceBroker = sourceBroker;
        this.socketTimeout = socketTimeout;
        this.socketBufferSize = socketBufferSize;
        this.fetchSize = fetchSize;
        this.fetcherBrokerId = fetcherBrokerId;
        this.maxWait = maxWait;
        this.minBytes = minBytes;
        this.isInterruptible = isInterruptible;

        init();
    }

    protected void init() {
        simpleConsumer = new SimpleConsumer(sourceBroker.host, sourceBroker.port, socketTimeout, socketBufferSize, clientId);
        brokerInfo = "host_%s-port_%s".format(sourceBroker.host, sourceBroker.port);
        metricId = new ClientIdAndBroker(clientId, brokerInfo);
        fetcherStats = new FetcherStats(metricId);
        fetcherLagStats = new FetcherLagStats(metricId);
        fetchRequestBuilder = new FetchRequestBuilder().
                clientId(clientId).
                replicaId(fetcherBrokerId).
                maxWait(maxWait).
                minBytes(minBytes);
    }

    private Map<TopicAndPartition, Long> partitionMap = Maps.newHashMap(); // a (topic, partition) -> offset map
    private ReentrantLock partitionMapLock = new ReentrantLock();
    private Condition partitionMapCond = partitionMapLock.newCondition();
    public SimpleConsumer simpleConsumer;
    private String brokerInfo;
    private ClientIdAndBroker metricId;
    public FetcherStats fetcherStats;
    public FetcherLagStats fetcherLagStats;
    public FetchRequestBuilder fetchRequestBuilder;
    Logger logger = LoggerFactory.getLogger(AbstractFetcherThread.class);

    /* callbacks to be defined in subclass */

    // process fetched data
    public abstract void processPartitionData(TopicAndPartition topicAndPartition, long fetchOffset,
                                              FetchResponsePartitionData partitionData);

    // handle a partition whose offset is out of range and return a new fetch offset
    public abstract long handleOffsetOutOfRange(TopicAndPartition topicAndPartition);

    // deal with partitions with errors, potentially due to leadership changes
    public abstract void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions);

    @Override
    public void shutdown() {
        super.shutdown();
        simpleConsumer.close();
    }

    @Override
    public void doWork() {
        Utils.inLock(partitionMapLock, new Function0<Object>() {
            @Override
            public Object apply() {
                if (partitionMap.isEmpty())
                    Utils.await(partitionMapCond, 200L, TimeUnit.MILLISECONDS);

                Utils.foreach(partitionMap, new Callable2<TopicAndPartition, Long>() {
                    @Override
                    public void apply(TopicAndPartition topicAndPartition, Long offset) {
                        fetchRequestBuilder.addFetch(topicAndPartition.topic, topicAndPartition.partition,
                                offset, fetchSize);
                    }
                });
                return null;
            }
        });

        FetchRequest fetchRequest = fetchRequestBuilder.build();
        if (!fetchRequest.requestInfo.isEmpty())
            processFetchRequest(fetchRequest);
    }

    private void processFetchRequest(final FetchRequest fetchRequest) {
        final Set<TopicAndPartition> partitionsWithError = Sets.newHashSet();
        FetchResponse response = null;
        try {
            logger.trace("issuing to broker {} of fetch request {}", sourceBroker.id, fetchRequest);
            response = simpleConsumer.fetch(fetchRequest);
        } catch (Throwable t) {
            if (isRunning.get()) {
                logger.warn("Error in fetch {}", fetchRequest, t);
                synchronized (partitionMapLock) {
                    partitionsWithError.addAll(partitionMap.keySet());
                }
            }
        }
        fetcherStats.requestRate.mark();

        if (response != null) {
            // process fetched data
            final FetchResponse finalResponse = response;
            Utils.inLock(partitionMapLock, new Function0<Object>() {
                @Override
                public Object apply() {
                    Utils.foreach(finalResponse.data, new Callable2<TopicAndPartition, FetchResponsePartitionData>() {
                        @Override
                        public void apply(TopicAndPartition topicAndPartition, FetchResponsePartitionData partitionData) {
                            String topic = topicAndPartition.topic;
                            int partitionId = topicAndPartition.partition;
                            Long currentOffset = partitionMap.get(topicAndPartition);
                            // we append to the log if the current offset is defined and it is the same as the offset requested during fetch
                            if (currentOffset != null && fetchRequest.requestInfo.get(topicAndPartition).offset == currentOffset) {
                                switch (partitionData.error) {
                                    case ErrorMapping.NoError:
                                        try {
                                            ByteBufferMessageSet messages = (ByteBufferMessageSet) partitionData.messages;
                                            int validBytes = messages.validBytes();
                                            MessageAndOffset messageAndOffset = Utils.lastOption(Lists.newArrayList(messages.shallowIterator()));
                                            long newOffset = messageAndOffset != null ? messageAndOffset.nextOffset() : currentOffset;

                                            partitionMap.put(topicAndPartition, newOffset);
                                            fetcherLagStats.getFetcherLagStats(topic, partitionId).lag(partitionData.hw - newOffset);
                                            fetcherStats.byteRate.mark(validBytes);
                                            // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                                            processPartitionData(topicAndPartition, currentOffset, partitionData);
                                        } catch (InvalidMessageException ime) {
                                            // we log the error and continue. This ensures two things
                                            // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread down and cause other topic partition to also lag
                                            // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes can cause this), we simply continue and
                                            //    should get fixed in the subsequent fetches
                                            logger.warn("Found invalid messages during fetch for partition [{},{}] offset {} error {}", topic, partitionId, currentOffset, ime.getMessage());
                                        } catch (Throwable ex) {
                                            throw new KafkaException(ex, "error processing data for partition [%s,%d] offset %d",
                                                    topic, partitionId, currentOffset);
                                        }
                                        break;
                                    case ErrorMapping.OffsetOutOfRangeCode:
                                        try {
                                            long newOffset = handleOffsetOutOfRange(topicAndPartition);
                                            partitionMap.put(topicAndPartition, newOffset);
                                            logger.warn("Current offset {} for partition [{},{}] out of range; reset offset to {}",
                                                    currentOffset, topic, partitionId, newOffset);
                                        } catch (Throwable e) {
                                            logger.warn("Error getting offset for partition [{},{}] to broker {}", topic, partitionId, sourceBroker.id, e);
                                            partitionsWithError.add(topicAndPartition);
                                        }
                                        break;
                                    default:
                                        if (isRunning.get()) {
                                            logger.warn("Error for partition [{},{}] to broker {}:{}", topic, partitionId, sourceBroker.id,
                                                    ErrorMapping.exceptionFor(partitionData.error).getClass());
                                            partitionsWithError.add(topicAndPartition);
                                        }
                                }
                            }
                        }
                    });
                    return null;
                }
            });
        }

        if (partitionsWithError.size() > 0) {
            logger.debug("handling partitions with error for {}", partitionsWithError);
            handlePartitionsWithErrors(partitionsWithError);
        }
    }

    public void addPartitions(Map<TopicAndPartition, Long> partitionAndOffsets) {
        Utils.lockInterruptibly(partitionMapLock);
        try {
            for (Map.Entry<TopicAndPartition, Long> entry : partitionAndOffsets.entrySet()) {
                TopicAndPartition topicAndPartition = entry.getKey();
                long offset = entry.getValue();
                // If the partitionMap already has the topic/partition, then do not update the map with the old offset
                if (!partitionMap.containsKey(topicAndPartition))
                    partitionMap.put(topicAndPartition,
                            PartitionTopicInfo.isOffsetInvalid(offset) ? handleOffsetOutOfRange(topicAndPartition) : offset);
            }
            partitionMapCond.signalAll();
        } finally {
            partitionMapLock.unlock();
        }
    }

    public void removePartitions(Set<TopicAndPartition> topicAndPartitions)  {
        Utils.lockInterruptibly(partitionMapLock);
        try {
            Utils.foreach(topicAndPartitions, new Callable1<TopicAndPartition>() {
                @Override
                public void apply(TopicAndPartition tp) {
                    partitionMap.remove(tp);
                }
            });
        } finally {
            partitionMapLock.unlock();
        }
    }

    public int partitionCount()  {
        Utils.lockInterruptibly(partitionMapLock);
        try {
            return partitionMap.size();
        } finally {
            partitionMapLock.unlock();
        }
    }


    static class FetcherStats extends KafkaMetricsGroup {
        public ClientIdAndBroker metricId;

        FetcherStats(ClientIdAndBroker metricId) {
            this.metricId = metricId;

            requestRate = newMeter(metricId + "-RequestsPerSec", "requests", TimeUnit.SECONDS);
            byteRate = newMeter(metricId + "-BytesPerSec", "bytes", TimeUnit.SECONDS);
        }

        public Meter requestRate;
        public Meter byteRate;
    }

    static class FetcherLagStats {
        public ClientIdAndBroker metricId;

        FetcherLagStats(ClientIdAndBroker metricId) {
            this.metricId = metricId;
        }

        private Function<ClientIdBrokerTopicPartition, FetcherLagMetrics> valueFactory = new Function<ClientIdBrokerTopicPartition, AbstractFetcherThread.FetcherLagMetrics>() {
            @Override
            public AbstractFetcherThread.FetcherLagMetrics apply(ClientIdBrokerTopicPartition k) {
                return new FetcherLagMetrics(k);
            }
        };
        public Pool<ClientIdBrokerTopicPartition, FetcherLagMetrics> stats = new Pool<ClientIdBrokerTopicPartition, FetcherLagMetrics>(valueFactory);

        public FetcherLagMetrics getFetcherLagStats(String topic, int partitionId) {
            return stats.getAndMaybePut(new ClientIdBrokerTopicPartition(metricId.clientId, metricId.brokerInfo, topic, partitionId));
        }
    }


    static class FetcherLagMetrics extends KafkaMetricsGroup {
        public ClientIdBrokerTopicPartition metricId;

        FetcherLagMetrics(ClientIdBrokerTopicPartition metricId) {
            this.metricId = metricId;

            newGauge(
                    metricId + "-ConsumerLag",
                    new Gauge<Long>() {
                        @Override
                        public Long value() {
                            return lagVal.get();
                        }
                    });
        }

        public AtomicLong lagVal = new AtomicLong(-1L);


        public void lag(long newLag) {
            lagVal.set(newLag);
        }

        public long lag() {
            return lagVal.get();
        }
    }


    static class ClientIdBrokerTopicPartition {
        public String clientId, brokerInfo, topic;
        public int partitionId;

        ClientIdBrokerTopicPartition(String clientId, String brokerInfo, String topic, int partitionId) {
            this.clientId = clientId;
            this.brokerInfo = brokerInfo;
            this.topic = topic;
            this.partitionId = partitionId;
        }

        @Override
        public String toString() {
            return String.format("%s-%s-%s-%d", clientId, brokerInfo, topic, partitionId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClientIdBrokerTopicPartition that = (ClientIdBrokerTopicPartition) o;

            if (partitionId != that.partitionId) return false;
            if (brokerInfo != null ? !brokerInfo.equals(that.brokerInfo) : that.brokerInfo != null) return false;
            if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null) return false;
            if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = clientId != null ? clientId.hashCode() : 0;
            result = 31 * result + (brokerInfo != null ? brokerInfo.hashCode() : 0);
            result = 31 * result + (topic != null ? topic.hashCode() : 0);
            result = 31 * result + partitionId;
            return result;
        }
    }
}
