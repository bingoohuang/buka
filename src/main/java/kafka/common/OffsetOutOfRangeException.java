package kafka.common;

public class OffsetOutOfRangeException extends KafkaException {
    public OffsetOutOfRangeException() {
        this(null);
    }

    public OffsetOutOfRangeException(String message) {
        super(message);
    }
}
