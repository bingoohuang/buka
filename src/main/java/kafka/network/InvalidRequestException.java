package kafka.network;

import kafka.common.KafkaException;

public class InvalidRequestException extends KafkaException {
    public InvalidRequestException(String format, Object... args) {
        super(format, args);
    }
}
