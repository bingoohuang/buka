package kafka.common;

import kafka.common.KafkaException;

/**
 * This exception is thrown when new leader election is not necessary.
 */
public class LeaderElectionNotNeededException extends KafkaException {
    public LeaderElectionNotNeededException(String format, Object... args) {
        super(format, args);
    }

}
