package kafka.common;

/**
 * Indicates the client has requested a range no longer available on the server
 */
public class UnknownCodecException extends KafkaException {
    public UnknownCodecException(String message) {
        super(message);
    }

    public UnknownCodecException(String message, Object... args) {
        super(message, args);
    }
}
