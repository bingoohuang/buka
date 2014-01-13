package kafka.consumer;

import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public class KafkaStream<K, V> implements Iterable<MessageAndMetadata<K, V>> {
    private BlockingQueue<FetchedDataChunk> queue;
    public int consumerTimeoutMs;
    private Decoder<K> keyDecoder;
    private Decoder<V> valueDecoder;
    public String clientId;

    public KafkaStream(BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs, Decoder<K> keyDecoder, Decoder<V> valueDecoder, String clientId) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
        this.clientId = clientId;

        iter =
                new ConsumerIterator<K, V>(queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId);
    }

    private ConsumerIterator<K, V> iter;

    /**
     * Create an iterator over messages in the stream.
     */
    @Override
    public Iterator<MessageAndMetadata<K, V>> iterator() {
        return iter;
    }

    /**
     * This method clears the queue being iterated during the consumer rebalancing. This is mainly
     * to reduce the number of duplicates received by the consumer
     */
    public void clear() {
        iter.clearCurrentChunk();
    }
}
