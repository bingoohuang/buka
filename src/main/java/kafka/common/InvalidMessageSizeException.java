package kafka.common;

public class InvalidMessageSizeException extends KafkaException {
    public InvalidMessageSizeException() {
        this(null);
    }
    public InvalidMessageSizeException(String message) {
        super(message);
    }
}
