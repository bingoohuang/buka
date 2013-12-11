package kafka.cluster;

import kafka.common.KafkaException;
import kafka.log.Log;
import kafka.server.ReplicaManagers;
import kafka.utils.SystemTime;
import kafka.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class Replica {
    public final int brokerId;
    public final Partition partition;
    public final Time time/* = SystemTime.instance*/;
    public final long initialHighWatermarkValue /* = 0L*/;
    public Log log;

    public Replica(int brokerId,
                   Partition partition) {
        this(brokerId, partition, SystemTime.instance, 0L, null);
    }

    public Replica(int brokerId,
                   Partition partition,
                   Time time,
                   long initialHighWatermarkValue,
                   Log log) {
        this.brokerId = brokerId;
        this.partition = partition;
        this.time = time;
        this.initialHighWatermarkValue = initialHighWatermarkValue;
        this.log = log;

        highWatermarkValue = new AtomicLong(initialHighWatermarkValue);
        logEndOffsetValue = new AtomicLong(ReplicaManagers.UnknownLogEndOffset);
        logEndOffsetUpdateTimeMsValue = new AtomicLong(time.milliseconds());

        topic = partition.topic;
        partitionId = partition.partitionId;
    }

    //only defined in local replica
    private AtomicLong highWatermarkValue;
    // only used for remote replica; logEndOffsetValue for local replica is kept in log
    private AtomicLong logEndOffsetValue;
    private AtomicLong logEndOffsetUpdateTimeMsValue;
    public String topic;
    public int partitionId;

    Logger logger = LoggerFactory.getLogger(Replica.class);

    public void logEndOffset(long newLogEndOffset) {
        if (!isLocal()) {
            logEndOffsetValue.set(newLogEndOffset);
            logEndOffsetUpdateTimeMsValue.set(time.milliseconds());
            logger.trace("Setting log end offset for replica {} for partition [{},{}] to {}",
                    brokerId, topic, partitionId, logEndOffsetValue.get());
        } else
            throw new KafkaException("Shouldn't set logEndOffset for replica %d partition [%s,%d] since it's local",
                    brokerId, topic, partitionId);

    }

    public long logEndOffset() {
        if (isLocal())
            return log.logEndOffset();
        else
            return logEndOffsetValue.get();
    }

    public boolean isLocal() {
        return log != null;
    }

    public long logEndOffsetUpdateTimeMs() {
        return logEndOffsetUpdateTimeMsValue.get();
    }

    public void highWatermark(long newHighWatermark) {
        if (isLocal()) {
            logger.trace("Setting hw for replica {} partition [{},{}] on broker {} to {}"
                    , brokerId, topic, partitionId, brokerId, newHighWatermark);
            highWatermarkValue.set(newHighWatermark);
        } else
            throw new KafkaException("Unable to set highwatermark for replica %d partition [%s,%d] since it's not local",
                    brokerId, topic, partitionId);
    }

    public long highWatermark() {
        if (isLocal())
            return highWatermarkValue.get();

        throw new KafkaException("Unable to get highwatermark for replica %d partition [%s,%d] since it's not local",
                brokerId, topic, partitionId);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (!(that instanceof Replica)) return false;

        Replica other = (Replica) that;
        return (topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition));
    }

    @Override
    public int hashCode() {
        return 31 + topic.hashCode() + 17 * brokerId + partition.hashCode();
    }


    @Override
    public String toString() {
        StringBuilder replicaString = new StringBuilder();
        replicaString.append("ReplicaId: " + brokerId);
        replicaString.append("; Topic: " + topic);
        replicaString.append("; Partition: " + partition.partitionId);
        replicaString.append("; isLocal: " + isLocal());
        if (isLocal()) replicaString.append("; Highwatermark: " + highWatermark());
        return replicaString.toString();
    }

}
