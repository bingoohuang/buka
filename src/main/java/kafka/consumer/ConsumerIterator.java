package kafka.consumer;

import kafka.common.KafkaException;
import kafka.common.MessageSizeTooLargeException;
import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;
import kafka.serializer.Decoder;
import kafka.utils.IteratorTemplate;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 */
public class ConsumerIterator<K, V> extends IteratorTemplate<MessageAndMetadata<K, V>> {
    private BlockingQueue<FetchedDataChunk> channel;
    int consumerTimeoutMs;
    private Decoder<K> keyDecoder;
    private Decoder<V> valueDecoder;
    String clientId;

    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder, String clientId) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;
        consumerTopicStats = ConsumerTopicStatsRegistry.getConsumerTopicStat(clientId);
    }

    private AtomicReference<Iterator<MessageAndOffset>> current = new AtomicReference(null);
    private PartitionTopicInfo currentTopicInfo = null;
    private Long consumedOffset = -1L;
    private ConsumerTopicStatsRegistry.ConsumerTopicStats consumerTopicStats;
    Logger logger = LoggerFactory.getLogger(ConsumerIterator.class);

    @Override
    public MessageAndMetadata<K, V> next() {
        MessageAndMetadata<K, V> item = super.next();
        if (consumedOffset < 0)
            throw new KafkaException("Offset returned by the message set is invalid %d", consumedOffset);
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        String topic = currentTopicInfo.topic;
        logger.trace("Setting {} consumed offset to {}", topic, consumedOffset);
        consumerTopicStats.getConsumerTopicStats(topic).messageRate.mark();
        consumerTopicStats.getConsumerAllTopicStats().messageRate.mark();
        return item;
    }

    protected MessageAndMetadata<K, V> makeNext() {
        FetchedDataChunk currentDataChunk = null;
        // if we don't have an iterator, get one
        Iterator<MessageAndOffset> localCurrent = current.get();
        if (localCurrent == null || !localCurrent.hasNext()) {
            if (consumerTimeoutMs < 0)
                currentDataChunk = Utils.take(channel);
            else {
                currentDataChunk = Utils.poll(channel, consumerTimeoutMs, TimeUnit.MILLISECONDS);
                if (currentDataChunk == null) {
                    // reset state to make the iterator re-iterable
                    resetState();
                    throw new ConsumerTimeoutException();
                }
            }
            if (currentDataChunk.equals(ZookeeperConsumerConnector.shutdownCommand)) {
                logger.debug("Received the shutdown command");
                channel.offer(currentDataChunk);
                return allDone();
            } else {
                currentTopicInfo = currentDataChunk.topicInfo;
                long cdcFetchOffset = currentDataChunk.fetchOffset;
                long ctiConsumeOffset = currentTopicInfo.getConsumeOffset();
                if (ctiConsumeOffset < cdcFetchOffset) {
                    logger.error("consumed offset: {} doesn't match fetch offset: {} for {};\n Consumer may lose data",
                            ctiConsumeOffset, cdcFetchOffset, currentTopicInfo);
                    currentTopicInfo.resetConsumeOffset(cdcFetchOffset);
                }
                localCurrent = currentDataChunk.messages.iterator();

                current.set(localCurrent);
            }
            // if we just updated the current chunk and it is empty that means the fetch size is too small!
            if (currentDataChunk.messages.validBytes() == 0)
                throw new MessageSizeTooLargeException("Found a message larger than the maximum fetch size of this consumer on topic " +
                        "%s partition %d at fetch offset %d. Increase the fetch size, or decrease the maximum message size the broker will allow.",
                        currentDataChunk.topicInfo.topic, currentDataChunk.topicInfo.partitionId, currentDataChunk.fetchOffset);
        }
        MessageAndOffset item = localCurrent.next();
        // reject the messages that have already been consumed
        while (item.offset < currentTopicInfo.getConsumeOffset() && localCurrent.hasNext()) {
            item = localCurrent.next();
        }
        consumedOffset = item.nextOffset();

        item.message.ensureValid(); // validate checksum of message to ensure it is valid

        return new MessageAndMetadata(currentTopicInfo.topic, currentTopicInfo.partitionId, item.message, item.offset, keyDecoder, valueDecoder);
    }

    public void clearCurrentChunk() {
        logger.debug("Clearing the current data chunk for this consumer iterator");
        current.set(null);
    }

    public static class ConsumerTimeoutException extends KafkaException {

        public ConsumerTimeoutException() {
            super("");
        }
    }
}
