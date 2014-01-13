package kafka.consumer;

import com.google.common.collect.Lists;
import kafka.message.ByteBufferMessageSet;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionTopicInfo {
    public static final long InvalidOffset = -1L;

    public static boolean isOffsetInvalid(long offset) {
        return offset < 0L;
    }

    public String topic;
    public int partitionId;
    private BlockingQueue<FetchedDataChunk> chunkQueue;
    private AtomicLong consumedOffset;
    private AtomicLong fetchedOffset;
    private AtomicInteger fetchSize;
    private String clientId;

    public PartitionTopicInfo(String topic,
                              int partitionId,
                              BlockingQueue<FetchedDataChunk> chunkQueue,
                              AtomicLong consumedOffset,
                              AtomicLong fetchedOffset,
                              AtomicInteger fetchSize,
                              String clientId) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;
        this.clientId = clientId;

        logger.debug("initial consumer offset of {} is {}", this, consumedOffset.get());
        logger.debug("initial fetch offset of {} is {}", this, fetchedOffset.get());
        consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId);
    }

    Logger logger = LoggerFactory.getLogger(PartitionTopicInfo.class);


    private ConsumerTopicStatsRegistry.ConsumerTopicStats consumerTopicStats;

    public long getConsumeOffset() {
        return consumedOffset.get();
    }

    public long getFetchOffset() {
        return fetchedOffset.get();
    }

    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        logger.debug("reset consume offset of {} to {}", this, newConsumeOffset);
    }

    public void resetFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
        logger.debug("reset fetch offset of ({}) to {}", this, newFetchOffset);
    }

    /**
     * Enqueue a message set for processing.
     */
    public void enqueue(ByteBufferMessageSet messages) {
        int size = messages.validBytes();
        if (size > 0) {
            long next = Utils.last(Lists.newArrayList(messages.shallowIterator())).nextOffset();
            logger.trace("Updating fetch offset = {} to {}", fetchedOffset.get(), next);
            Utils.put(chunkQueue, new FetchedDataChunk(messages, this, fetchedOffset.get()));
            fetchedOffset.set(next);
            logger.debug("updated fetch offset of ({}) to {}", this, next);
            consumerTopicStats.getConsumerTopicStats(topic).byteRate.mark(size);
            consumerTopicStats.getConsumerAllTopicStats().byteRate.mark(size);
        } else if (messages.sizeInBytes() > 0) {
            Utils.put(chunkQueue, new FetchedDataChunk(messages, this, fetchedOffset.get()));
        }
    }

    @Override
    public String toString() {
        return topic + ":" + partitionId + ": fetched offset = " + fetchedOffset.get() +
                ": consumed offset = " + consumedOffset.get();
    }
}
