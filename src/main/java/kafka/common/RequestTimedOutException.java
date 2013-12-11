package kafka.common;

public class RequestTimedOutException extends KafkaException {
    public RequestTimedOutException() {
        this(null);
    }
    public RequestTimedOutException(String message) {
        super(message);
    }
}
