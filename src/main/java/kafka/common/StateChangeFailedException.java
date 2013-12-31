package kafka.common;

public class StateChangeFailedException extends KafkaException {
    public StateChangeFailedException(String format, Object... args) {
        super(format, args);
    }
}
