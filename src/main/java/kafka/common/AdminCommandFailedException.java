package kafka.common;

public class AdminCommandFailedException extends KafkaException {
    public AdminCommandFailedException(String format, Object... args) {
        super(format, args);
    }

    public AdminCommandFailedException(Throwable e, String format, Object... args) {
        super(e, format, args);
    }
}
