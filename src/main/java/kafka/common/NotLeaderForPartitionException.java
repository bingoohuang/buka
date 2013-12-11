package kafka.common;

public class NotLeaderForPartitionException extends KafkaException {
    public NotLeaderForPartitionException() {
        this(null);
    }
    public NotLeaderForPartitionException(String message) {
        super(message);
    }
}
