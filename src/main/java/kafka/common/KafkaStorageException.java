package kafka.common;

import java.io.IOException;

public class KafkaStorageException extends KafkaException {
    public KafkaStorageException(String format, Object... args) {
        super(format, args);
    }

    public KafkaStorageException(IOException e, String format, Object... args) {
        super(e, format, args);
    }

    public KafkaStorageException(Throwable e, String format, Object... args) {
        super(e, format, args);
    }
}
