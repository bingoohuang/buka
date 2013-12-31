package kafka.common;

public class AdminCommandFailedException extends KafkaException {
    public AdminCommandFailedException(Throwable e, String format, Object... args) {
        super(e, format, args);
    }
}
