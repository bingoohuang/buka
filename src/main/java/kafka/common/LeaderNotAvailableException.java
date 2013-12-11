package kafka.common;

public class LeaderNotAvailableException extends KafkaException {
    public LeaderNotAvailableException() {
        this(null);
    }
    public LeaderNotAvailableException(String message) {
        super(message);
    }
}
