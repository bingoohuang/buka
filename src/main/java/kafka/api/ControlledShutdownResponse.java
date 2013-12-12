package kafka.api;

import com.google.common.collect.Sets;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.utils.Function1;
import kafka.utils.Function2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.util.Set;

import static kafka.api.ApiUtils.readShortString;
import static kafka.api.ApiUtils.writeShortString;

public class ControlledShutdownResponse extends RequestOrResponse {

    public static ControlledShutdownResponse readFrom(ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();

        Set<TopicAndPartition> partitionsRemaining = Sets.newHashSet();
        for (int i = 0; i < numEntries; ++i) {
            String topic = readShortString(buffer);
            int partition = buffer.getInt();
            partitionsRemaining.add(new TopicAndPartition(topic, partition));
        }

        return new ControlledShutdownResponse(correlationId, errorCode, partitionsRemaining);
    }

    public short errorCode; /* ErrorMapping.NoError,*/
    public Set<TopicAndPartition> partitionsRemaining;


    public ControlledShutdownResponse(int correlationId, Set<TopicAndPartition> partitionsRemaining) {
        this(correlationId, ErrorMapping.NoError, partitionsRemaining);
    }

    public ControlledShutdownResponse(int correlationId, short errorCode, Set<TopicAndPartition> partitionsRemaining) {
        super(correlationId);

        this.errorCode = errorCode;
        this.partitionsRemaining = partitionsRemaining;

    }

    @Override
    public int sizeInBytes() {
        return 4 /* correlation id */ +
                2 /* error code */ +
                4 /* number of responses */
                + Utils.foldLeft(partitionsRemaining, 0, new Function2<Integer, TopicAndPartition, Integer>() {
            @Override
            public Integer apply(Integer arg1, TopicAndPartition topicAndPartition) {
                return arg1 + 2 + topicAndPartition.topic.length() /* topic */ +
                        4 /* partition */;
            }
        });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(partitionsRemaining.size());

        Utils.foreach(partitionsRemaining, new Function1<TopicAndPartition, Void>() {
            @Override
            public Void apply(TopicAndPartition topicAndPartition) {
                writeShortString(buffer, topicAndPartition.topic);
                buffer.putInt(topicAndPartition.partition);
                return null;
            }
        });
    }
}
