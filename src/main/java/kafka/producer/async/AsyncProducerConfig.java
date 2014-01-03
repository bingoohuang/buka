package kafka.producer.async;

import kafka.producer.SyncProducerConfig;
import kafka.utils.VerifiableProperties;

public abstract class AsyncProducerConfig extends SyncProducerConfig {
    protected AsyncProducerConfig(VerifiableProperties props) {
        super(props);

        init();
    }

    private void init() {
        queueBufferingMaxMs = props.getInt("queue.buffering.max.ms", 5000);
        queueBufferingMaxMessages = props.getInt("queue.buffering.max.messages", 10000);
        queueEnqueueTimeoutMs = props.getInt("queue.enqueue.timeout.ms", -1);
        batchNumMessages = props.getInt("batch.num.messages", 200);
        serializerClass = props.getString("serializer.class", "kafka.serializer.DefaultEncoder");
        keySerializerClass = props.getString("key.serializer.class", serializerClass);
    }


    /* maximum time, in milliseconds, for buffering data on the producer queue */
    public int queueBufferingMaxMs;

    /**
     * the maximum size of the blocking queue for buffering on the producer
     */
    public int queueBufferingMaxMessages;

    /**
     * Timeout for event enqueue:
     * 0: events will be enqueued immediately or dropped if the queue is full
     * -ve: enqueue will block indefinitely if the queue is full
     * +ve: enqueue will block up to this many milliseconds if the queue is full
     */
    public int queueEnqueueTimeoutMs;

    /**
     * the number of messages batched at the producer
     */
    public int batchNumMessages;

    /**
     * the serializer class for values
     */
    public String serializerClass;

    /**
     * the serializer class for keys (defaults to the same as for values)
     */
    public String keySerializerClass;
}
