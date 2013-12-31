package kafka.common;

public class TopicExistsException extends KafkaException {
    public TopicExistsException(String format, Object... args) {
        super(format, args);
    }
}
