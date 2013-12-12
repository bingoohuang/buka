package kafka.api;

import kafka.network.MultiSend;
import kafka.network.Send;
import kafka.utils.Function2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;

import static kafka.api.ApiUtils.writeShortString;

public class TopicDataSend extends Send {
    public TopicData topicData;

    public static class TopicDataMultiSend extends MultiSend<PartitionDataSend> {
        protected TopicDataMultiSend(TopicData topicData, List<PartitionDataSend> sends) {
            super(sends);
            expectedBytesToWrite = topicData.sizeInBytes() - topicData.headerSize();
        }
    }

    public TopicDataSend(TopicData topicData) {
        this.topicData = topicData;
        size = topicData.sizeInBytes();

        buffer = ByteBuffer.allocate(topicData.headerSize());

        writeShortString(buffer, topicData.topic);
        buffer.putInt(topicData.partitionData.size());
        buffer.rewind();

        List<PartitionDataSend> sendData = Utils.mapList(topicData.partitionData,
                new Function2<Integer, FetchResponsePartitionData, PartitionDataSend>() {
                    @Override
                    public PartitionDataSend apply(Integer arg1, FetchResponsePartitionData arg2) {
                        return new PartitionDataSend(arg1, arg2);
                    }
                });
        sends = new TopicDataMultiSend(topicData, sendData);
    }

    private int size;

    private int sent = 0;

    @Override
    public boolean complete() {
        return sent >= size;
    }

    private ByteBuffer buffer;
    public MultiSend<PartitionDataSend> sends;

    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int written = 0;
        if (buffer.hasRemaining()) ;
        written += Utils.write(channel, buffer);
        if (!buffer.hasRemaining() && !sends.complete()) {
            written += sends.writeTo(channel);
        }
        sent += written;
        return written;
    }
}
