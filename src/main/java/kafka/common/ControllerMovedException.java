package kafka.common;

public class ControllerMovedException extends KafkaException {
    public ControllerMovedException() {
        this(null);
    }

    public ControllerMovedException(String message) {
        super(message);
    }
}
