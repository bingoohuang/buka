package kafka.common;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import kafka.message.InvalidMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * A bi-directional mapping between error codes and exceptions x
 */
public abstract class ErrorMapping {
    public static final ByteBuffer EmptyByteBuffer = ByteBuffer.allocate(0);

    public static final short UnknownCode = -1;
    public static final short NoError = 0;
    public static final short OffsetOutOfRangeCode = 1;
    public static final short InvalidMessageCode = 2;
    public static final short UnknownTopicOrPartitionCode = 3;
    public static final short InvalidFetchSizeCode = 4;
    public static final short LeaderNotAvailableCode = 5;
    public static final short NotLeaderForPartitionCode = 6;
    public static final short RequestTimedOutCode = 7;
    public static final short BrokerNotAvailableCode = 8;
    public static final short ReplicaNotAvailableCode = 9;
    public static final short MessageSizeTooLargeCode = 10;
    public static final short StaleControllerEpochCode = 11;
    public static final short OffsetMetadataTooLargeCode = 12;
    public static final short StaleLeaderEpochCode = 13;

    private static BiMap<Class<? extends Throwable>, Short> exceptionToCode;

    static {
        exceptionToCode.put(OffsetOutOfRangeException.class, OffsetOutOfRangeCode);
        exceptionToCode.put(InvalidMessageException.class, InvalidMessageCode);
        exceptionToCode.put(UnknownTopicOrPartitionException.class, UnknownTopicOrPartitionCode);
        exceptionToCode.put(InvalidMessageSizeException.class, InvalidFetchSizeCode);
        exceptionToCode.put(NotLeaderForPartitionException.class, NotLeaderForPartitionCode);
        exceptionToCode.put(LeaderNotAvailableException.class, LeaderNotAvailableCode);
        exceptionToCode.put(RequestTimedOutException.class, RequestTimedOutCode);
        exceptionToCode.put(BrokerNotAvailableException.class, BrokerNotAvailableCode);
        exceptionToCode.put(ReplicaNotAvailableException.class, ReplicaNotAvailableCode);
        exceptionToCode.put(MessageSizeTooLargeException.class, MessageSizeTooLargeCode);
        exceptionToCode.put(ControllerMovedException.class, StaleControllerEpochCode);
        exceptionToCode.put(OffsetMetadataTooLargeException.class, OffsetMetadataTooLargeCode);
    }

    public static Short codeFor(Class<? extends Throwable> exception) {
        return Objects.firstNonNull(exceptionToCode.get(exception), UnknownCode);
    }

    public static void maybeThrowException(Short code) {
        Throwable throwable = exceptionFor(code);
        if (throwable != null) {
            throw Throwables.propagate(throwable);
        }
    }

    static Logger logger = LoggerFactory.getLogger(ErrorMapping.class);

    public static KafkaException exceptionFor(Short code) {
        Class<? extends Throwable> throwable = exceptionToCode.inverse().get(code);
        if (throwable == null) return null;

        try {
            return (KafkaException) throwable.newInstance();
        } catch (Exception e) {
            logger.error("create instance of {} error", throwable, e);
        }
        return null;
    }
}
