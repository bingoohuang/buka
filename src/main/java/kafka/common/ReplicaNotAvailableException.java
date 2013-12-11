package kafka.common;

public class ReplicaNotAvailableException extends KafkaException {
    public ReplicaNotAvailableException() {
        this(null);
    }
    public ReplicaNotAvailableException(String message) {
        super(message);
    }
}
