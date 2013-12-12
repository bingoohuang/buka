package kafka.common;

import java.io.IOException;

public class KafkaException extends RuntimeException {
    public KafkaException(String message) {
        super(message);
    }

    public KafkaException(String format, Object... args) {
        super(String.format(format, args));
    }

    public KafkaException(Throwable e, String format, Object... args) {
        super(String.format(format, args), e);
    }

    public KafkaException(IOException e) {
        super(e);
    }

    public KafkaException(Throwable e) {
        super(e);
    }
}