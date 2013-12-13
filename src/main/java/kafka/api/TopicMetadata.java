package kafka.api;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static kafka.api.ApiUtils.*;

public class TopicMetadata {
    public static final int NoLeaderNodeId = -1;

    public static TopicMetadata readFrom(final ByteBuffer buffer, final Map<Integer, Broker> brokers) {
        short errorCode = readShortInRange(buffer, "error code", Range.make((short) -1, Short.MAX_VALUE));
        String topic = readShortString(buffer);
        int numPartitions = readIntInRange(buffer, "number of partitions", Range.make(0, Integer.MAX_VALUE));
        List<PartitionMetadata> partitionsMetadata = Utils.flatList(0, numPartitions, new Function1<Integer, PartitionMetadata>() {
            @Override
            public PartitionMetadata apply(Integer arg) {
                return PartitionMetadata.readFrom(buffer, brokers);
            }
        });

        return new TopicMetadata(topic, partitionsMetadata, errorCode);
    }

    public String topic;
    public List<PartitionMetadata> partitionsMetadata;
    public short errorCode;

    public TopicMetadata(String topic, List<PartitionMetadata> partitionsMetadata) {
        this(topic, partitionsMetadata, ErrorMapping.NoError);
    }

    public TopicMetadata(String topic, List<PartitionMetadata> partitionsMetadata, short errorCode) {
        this.topic = topic;
        this.partitionsMetadata = partitionsMetadata;
        this.errorCode = errorCode;
    }

    public int sizeInBytes() {
        return 2 /* error code */ +
                shortStringLength(topic) +
                4
                + Utils.foldLeft(partitionsMetadata, 0, new Function2<Integer, PartitionMetadata, Integer>() {
            @Override
            public Integer apply(Integer arg1, PartitionMetadata _) {
                return arg1 + _.sizeInBytes(); /* size and partition data array */
            }
        });
    }

    public void writeTo(final ByteBuffer buffer) {
    /* error code */
        buffer.putShort(errorCode);
    /* topic */
        writeShortString(buffer, topic);
    /* number of partitions */
        buffer.putInt(partitionsMetadata.size());

        Utils.foreach(partitionsMetadata, new Callable1<PartitionMetadata>() {
            @Override
            public void apply(PartitionMetadata arg) {
                arg.writeTo(buffer);
            }
        });
    }

    @Override
    public String toString() {
        final StringBuilder topicMetadataInfo = new StringBuilder();
        topicMetadataInfo.append(String.format("{TopicMetadata for topic %s -> ", topic));
        if (errorCode == ErrorMapping.NoError) {
            Utils.foreach(partitionsMetadata, new Callable1<PartitionMetadata>() {
                @Override
                public void apply(PartitionMetadata partitionMetadata) {
                    switch (partitionMetadata.errorCode) {
                        case ErrorMapping.NoError:
                            topicMetadataInfo.append(String.format("\nMetadata for partition [%s,%d] is %s", topic,
                                    partitionMetadata.partitionId, partitionMetadata.toString()));
                            break;
                        case ErrorMapping.ReplicaNotAvailableCode:
                            // this error message means some replica other than the leader is not available. The consumer
                            // doesn't care about non leader replicas, so ignore this
                            topicMetadataInfo.append(String.format("\nMetadata for partition [%s,%d] is %s", topic,
                                    partitionMetadata.partitionId, partitionMetadata.toString()));
                            break;
                        default:
                            topicMetadataInfo.append(String.format("\nMetadata for partition [%s,%d] is not available due to %s", topic,
                                    partitionMetadata.partitionId, ErrorMapping.exceptionFor(partitionMetadata.errorCode).getClass().getName()));
                    }
                }
            });

        } else {
            topicMetadataInfo.append(String.format("\nNo partition metadata for topic %s due to %s", topic,
                    ErrorMapping.exceptionFor(errorCode).getClass().getName()));
        }
        topicMetadataInfo.append("}");
        return topicMetadataInfo.toString();
    }
}
