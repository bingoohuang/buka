package kafka.api;

import kafka.network.Send;
import kafka.utils.Utils;

import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

public class PartitionDataSend extends Send {
    public int partitionId;
    public FetchResponsePartitionData partitionData;

    public PartitionDataSend(int partitionId, FetchResponsePartitionData partitionData) {
        this.partitionId = partitionId;
        this.partitionData = partitionData;

        messageSize = partitionData.messages.sizeInBytes();
        buffer = ByteBuffer.allocate(4 /** partitionId **/ + FetchResponsePartitionData.headerSize);
        buffer.putInt(partitionId);
        buffer.putShort(partitionData.error);
        buffer.putLong(partitionData.hw);
        buffer.putInt(partitionData.messages.sizeInBytes());
        buffer.rewind();
    }

    private int messageSize;
    private int messagesSentSize = 0;

    private ByteBuffer buffer;


    @Override
    public boolean complete() {
        return !buffer.hasRemaining() && messagesSentSize >= messageSize;
    }

    @Override
    public int writeTo(GatheringByteChannel channel) {
        int written = 0;
        if (buffer.hasRemaining())
            written += Utils.write(channel, buffer);
        if (!buffer.hasRemaining() && messagesSentSize < messageSize) {
            int bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize);
            messagesSentSize += bytesSent;
            written += bytesSent;
        }
        return written;
    }


}
