package kafka.common;

public class OptimisticLockFailureException extends KafkaException {
    public OptimisticLockFailureException(String format, Object... args) {
        super(format, args);
    }
}
