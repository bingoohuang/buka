package kafka.common;

public class OffsetMetadataTooLargeException extends KafkaException {
    public OffsetMetadataTooLargeException() {
        this(null);
    }

    public OffsetMetadataTooLargeException(String message) {
        super(message);
    }
}
