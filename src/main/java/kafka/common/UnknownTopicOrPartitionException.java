package kafka.common;

public class UnknownTopicOrPartitionException extends KafkaException {
    public UnknownTopicOrPartitionException() {
        this(null);
    }
    public UnknownTopicOrPartitionException(String message) {
        super(message);
    }
}
