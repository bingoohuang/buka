package kafka.common;

public class InvalidTopicException extends KafkaException {

    public InvalidTopicException(String message) {
        super(message);
    }
}
