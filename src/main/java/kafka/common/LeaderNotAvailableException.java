package kafka.common;

public class LeaderNotAvailableException extends KafkaException {
    public LeaderNotAvailableException() {
        this(null);
    }
    public LeaderNotAvailableException(String message) {
        super(message);
    }

    public LeaderNotAvailableException(Throwable e, String format, Object... args) {
        super(e, format, args);
    }
}
