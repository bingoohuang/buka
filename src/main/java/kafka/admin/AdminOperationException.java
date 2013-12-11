package kafka.admin;

import kafka.common.KafkaException;

public class AdminOperationException extends KafkaException {
    public AdminOperationException(String message) {
        super(message);
    }
}
