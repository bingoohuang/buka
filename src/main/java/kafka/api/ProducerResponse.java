package kafka.api;

import com.google.common.collect.Table;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.*;

public class ProducerResponse extends RequestOrResponse {
    public static ProducerResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        int topicCount = buffer.getInt();

        Map<TopicAndPartition, ProducerResponseStatus> status =
                Utils.flatMaps(1, topicCount, new Function0<Map<TopicAndPartition, ProducerResponseStatus>>() {
                    @Override
                    public Map<TopicAndPartition, ProducerResponseStatus> apply() {
                        final String topic = readShortString(buffer);
                        int partitionCount = buffer.getInt();
                        return Utils.map(1, partitionCount, new Function0<Tuple2<TopicAndPartition, ProducerResponseStatus>>() {
                            @Override
                            public Tuple2<TopicAndPartition, ProducerResponseStatus> apply() {
                                int partition = buffer.getInt();
                                short error = buffer.getShort();
                                long offset = buffer.getLong();
                                return Tuple2.make(new TopicAndPartition(topic, partition),
                                        new ProducerResponseStatus(error, offset));
                            }
                        });
                    }
                });

        return new ProducerResponse(correlationId, status);
    }

    public Map<TopicAndPartition, ProducerResponseStatus> status;

    public ProducerResponse(int correlationId, Map<TopicAndPartition, ProducerResponseStatus> status) {
        super(correlationId);
        this.status = status;

        statusGroupedByTopic = Utils.groupBy(status, new Function2<TopicAndPartition, ProducerResponseStatus, String>() {
            @Override
            public String apply(TopicAndPartition arg1, ProducerResponseStatus arg2) {
                return arg1.topic;
            }
        });
    }

    /**
     * Partitions the status map into a map of maps (one for each topic).
     */
    private Table<String, TopicAndPartition, ProducerResponseStatus> statusGroupedByTopic;

    public boolean hasError() {
        return Utils.exists(status.values(), new Function1<ProducerResponseStatus, Boolean>() {
            @Override
            public Boolean apply(ProducerResponseStatus arg) {
                return arg.error != ErrorMapping.NoError;
            }
        });
    }

    public int sizeInBytes() {
        Table<String, TopicAndPartition, ProducerResponseStatus> groupedStatus = statusGroupedByTopic;
        return 4 + /* correlation id */
                4 + /* topic count */
                Utils.foldLeft(groupedStatus, 0, new Function3<Integer, String, Map<TopicAndPartition, ProducerResponseStatus>, Integer>() {
                    @Override
                    public Integer apply(Integer arg1, String topic, Map<TopicAndPartition, ProducerResponseStatus> arg3) {
                        return arg1 + shortStringLength(topic) +
                                4 + /* partition count for this topic */
                                arg3.size() * (
                                        4 + /* partition id */
                                                2 + /* error code */
                                                8 /* offset */
                                );
                    }
                });
    }


    public void writeTo(final ByteBuffer buffer) {
        Table<String, TopicAndPartition, ProducerResponseStatus> groupedStatus = statusGroupedByTopic;
        buffer.putInt(correlationId);
        buffer.putInt(groupedStatus.size()); // topic count

        Utils.foreach(groupedStatus, new Function2<String, Map<TopicAndPartition, ProducerResponseStatus>, Void>() {
            @Override
            public Void apply(String topic, Map<TopicAndPartition, ProducerResponseStatus> errorsAndOffsets) {
                writeShortString(buffer, topic);
                buffer.putInt(errorsAndOffsets.size()); // partition count
                Utils.foreach(errorsAndOffsets, new Function2<TopicAndPartition, ProducerResponseStatus, Void>() {
                    @Override
                    public Void apply(TopicAndPartition arg1, ProducerResponseStatus arg2) {
                        int partition = arg1.partition;
                        short error = arg2.error;
                        long nextOffset = arg2.offset;

                        buffer.putInt(partition);
                        buffer.putShort(error);
                        buffer.putLong(nextOffset);
                        return null;
                    }
                });

                return null;
            }
        });
    }
}
