package kafka.message;

import kafka.common.KafkaException;

public class InvalidMessageException extends KafkaException {
    public InvalidMessageException(String format, Object... args) {
        super(format, args);
    }

    public InvalidMessageException() {
        super("");
    }
}
