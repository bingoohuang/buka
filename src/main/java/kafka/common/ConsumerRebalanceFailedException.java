package kafka.common;

public class ConsumerRebalanceFailedException extends KafkaException {

    public ConsumerRebalanceFailedException(String message) {
        super(message);
    }
}
