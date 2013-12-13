package kafka.common;

public class InvalidOffsetException extends KafkaException {
    public InvalidOffsetException(String format, Object... args) {
        super(format, args);
    }
}
