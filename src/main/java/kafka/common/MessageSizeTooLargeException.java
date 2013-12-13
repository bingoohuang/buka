package kafka.common;

public class MessageSizeTooLargeException extends KafkaException {
    public MessageSizeTooLargeException() {
        this(null);
    }
    public MessageSizeTooLargeException(String message) {
        super(message);
    }

    public MessageSizeTooLargeException(String format, Object... args) {
        super(format, args);
    }
}
