package kafka.api;

import kafka.common.TopicAndPartition;
import kafka.network.MultiSend;
import kafka.network.Send;
import kafka.utils.Function2;
import kafka.utils.Tuple2;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;
import java.util.Map;

public class FetchResponseSend extends Send {
    public FetchResponse fetchResponse;


    public static class FetchResponseDataSend extends MultiSend<TopicDataSend> {
        protected FetchResponseDataSend(FetchResponse fetchResponse, List<TopicDataSend> sends) {
            super(sends);

            expectedBytesToWrite = fetchResponse.sizeInBytes() - FetchResponse.headerSize;
        }
    }

    public FetchResponseSend(FetchResponse fetchResponse) {
        this.fetchResponse = fetchResponse;
        size = fetchResponse.sizeInBytes();
        sendSize = 4 /* for size */ + size;
        buffer = ByteBuffer.allocate(4 /* for size */ + FetchResponse.headerSize);
        buffer.putInt(size);
        buffer.putInt(fetchResponse.correlationId);
        buffer.putInt(fetchResponse.dataGroupedByTopic.size()); // topic count
        buffer.rewind();

        sends = new FetchResponseDataSend(fetchResponse, Utils.mapList(fetchResponse.dataGroupedByTopic,
                new Function2<String, Map<TopicAndPartition, FetchResponsePartitionData>, TopicDataSend>() {
                    @Override
                    public TopicDataSend apply(String topic, Map<TopicAndPartition, FetchResponsePartitionData> data) {
                        return new TopicDataSend(new TopicData(topic, Utils.map(data, new Function2<TopicAndPartition, FetchResponsePartitionData, Tuple2<Integer, FetchResponsePartitionData>>() {
                            @Override
                            public Tuple2<Integer, FetchResponsePartitionData> apply(TopicAndPartition arg1, FetchResponsePartitionData arg2) {
                                return Tuple2.make(arg1.partition, arg2);
                            }
                        })));
                    }
                }
        ));
    }

    private int size;

    private int sent = 0;

    private int sendSize;

    @Override
    public boolean complete() {
        return sent >= sendSize;
    }

    private ByteBuffer buffer;


    public MultiSend<TopicDataSend> sends;

    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int written = 0;
        if (buffer.hasRemaining())
            written += Utils.write(channel, buffer);
        if (!buffer.hasRemaining() && !sends.complete()) {
            written += sends.writeTo(channel);
        }
        sent += written;
        return written;
    }
}
