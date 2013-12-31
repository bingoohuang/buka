package kafka.common;

public class ReplicaNotAvailableException extends KafkaException {
    public ReplicaNotAvailableException(String message) {
        super(message);
    }

    public ReplicaNotAvailableException(Throwable e) {
        super(e);
    }
}
