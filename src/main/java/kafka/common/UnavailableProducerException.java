package kafka.common;

public class UnavailableProducerException extends KafkaException {
    public UnavailableProducerException(String format, Object... args) {
        super(format, args);
    }
}
