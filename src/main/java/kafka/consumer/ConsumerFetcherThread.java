package kafka.consumer;

import com.google.common.collect.Sets;
import kafka.api.FetchResponsePartitionData;
import kafka.api.OffsetRequestReader;
import kafka.api.Requests;
import kafka.cluster.Broker;
import kafka.common.KafkaException;
import kafka.common.TopicAndPartition;
import kafka.message.ByteBufferMessageSet;
import kafka.server.AbstractFetcherThread;

import java.util.Map;

public class ConsumerFetcherThread extends AbstractFetcherThread {
    public String name;
    public ConsumerConfig config;
    public Broker sourceBroker;
    public Map<TopicAndPartition, PartitionTopicInfo> partitionMap;
    public ConsumerFetcherManager consumerFetcherManager;

    public ConsumerFetcherThread(String name, ConsumerConfig config, Broker sourceBroker, Map<TopicAndPartition, PartitionTopicInfo> partitionMap, ConsumerFetcherManager consumerFetcherManager) {
        super(/*name =*/ name,
               /* clientId = */config.clientId + "-" + name,
                /*sourceBroker =*/ sourceBroker,
               /* socketTimeout =*/ config.socketTimeoutMs,
               /* socketBufferSize = */config.socketReceiveBufferBytes,
                /*fetchSize =*/ config.fetchMessageMaxBytes,
               /* fetcherBrokerId =*/ Requests.OrdinaryConsumerId,
               /* maxWait = */config.fetchWaitMaxMs,
                /*minBytes = */config.fetchMinBytes,
               /* isInterruptible =*/ true);
        this.name = name;
        this.config = config;
        this.sourceBroker = sourceBroker;
        this.partitionMap = partitionMap;
        this.consumerFetcherManager = consumerFetcherManager;
    }

    // process fetched data
    @Override
    public void processPartitionData(TopicAndPartition topicAndPartition, long fetchOffset, FetchResponsePartitionData partitionData) {
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        if (pti.getFetchOffset() != fetchOffset)
            throw new KafkaException("Offset doesn't match for partition [%s,%d] pti offset: %d fetch offset: %d",
                    topicAndPartition.topic, topicAndPartition.partition, pti.getFetchOffset(), fetchOffset);
        pti.enqueue((ByteBufferMessageSet) partitionData.messages);
    }

    // handle a partition whose offset is out of range and return a new fetch offset
    @Override
    public long handleOffsetOutOfRange(TopicAndPartition topicAndPartition) {
        long startTimestamp = 0;
        if (config.autoOffsetReset.equals(OffsetRequestReader.SmallestTimeString)) {
            startTimestamp = OffsetRequestReader.EarliestTime;
        } else if (config.autoOffsetReset.equals(OffsetRequestReader.LargestTimeString)) {
            startTimestamp = OffsetRequestReader.LatestTime;
        } else {
            startTimestamp = OffsetRequestReader.LatestTime;
        }
        long newOffset = simpleConsumer.earliestOrLatestOffset(topicAndPartition, startTimestamp, Requests.OrdinaryConsumerId);
        PartitionTopicInfo pti = partitionMap.get(topicAndPartition);
        pti.resetFetchOffset(newOffset);
        pti.resetConsumeOffset(newOffset);
        return newOffset;
    }

    // any logic for partitions whose leader has changed
    @Override
    public void handlePartitionsWithErrors(Iterable<TopicAndPartition> partitions) {
        removePartitions(Sets.newHashSet(partitions));
        consumerFetcherManager.addPartitionsWithError(partitions);
    }
}
