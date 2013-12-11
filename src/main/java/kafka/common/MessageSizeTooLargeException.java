package kafka.common;

public class MessageSizeTooLargeException extends KafkaException {
    public MessageSizeTooLargeException() {
        this(null);
    }
    public MessageSizeTooLargeException(String message) {
        super(message);
    }
}
