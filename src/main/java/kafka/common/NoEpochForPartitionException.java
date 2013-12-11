package kafka.common;

/**
 * Thrown when a get epoch request is made for partition, but no epoch exists for that partition
 */
public class NoEpochForPartitionException extends KafkaException {

    public NoEpochForPartitionException(String format, Object... args) {
        super(format, args);
    }
}
