package kafka.message;

import kafka.serializer.Decoder;
import kafka.utils.Utils;

public class MessageAndMetadata<K, V> {
    public final String topic;
    public final int partition;
    private final Message rawMessage;
    public final long offset;
    public final Decoder<K> keyDecoder;
    public final Decoder<V> valueDecoder;

    public MessageAndMetadata(String topic,
                              int partition,
                              Message rawMessage,
                              long offset,
                              Decoder<K> keyDecoder,
                              Decoder<V> valueDecoder) {
        this.topic = topic;
        this.partition = partition;
        this.rawMessage = rawMessage;
        this.offset = offset;
        this.keyDecoder = keyDecoder;
        this.valueDecoder = valueDecoder;
    }

    /**
     * Return the decoded message key and payload
     */
    public K key() {
        if (rawMessage.key() == null) return null;

        return keyDecoder.fromBytes(Utils.readBytes(rawMessage.key()));
    }

    public V message() {
        if (rawMessage.isNull()) return null;

        return valueDecoder.fromBytes(Utils.readBytes(rawMessage.payload()));
    }
}
