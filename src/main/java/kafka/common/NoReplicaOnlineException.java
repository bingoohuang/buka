package kafka.common;

public class NoReplicaOnlineException extends KafkaException {
    public NoReplicaOnlineException(String format, Object... args) {
        super(format, args);
    }
}
