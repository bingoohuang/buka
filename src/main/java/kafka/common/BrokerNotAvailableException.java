package kafka.common;

public class BrokerNotAvailableException extends KafkaException {
    public BrokerNotAvailableException() {
        super("");
    }
    public BrokerNotAvailableException(String format, Object... args) {
        super(format, args);
    }
}
