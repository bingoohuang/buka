package kafka.common;

public class OffsetOutOfRangeException extends KafkaException {
    public OffsetOutOfRangeException() {
        this(null);
    }

    public OffsetOutOfRangeException(String message) {
        super(message);
    }

    public OffsetOutOfRangeException(String format, Object... args) {
        super(format, args);
    }
}
