package kafka.common;

public class InvalidConfigException extends KafkaException {
    public InvalidConfigException(String format, Object... args) {
        super(format, args);
    }

    public InvalidConfigException(String message) {
        super(message);
    }
}
