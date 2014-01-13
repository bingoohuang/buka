package kafka.consumer;

import kafka.message.ByteBufferMessageSet;

public class FetchedDataChunk {
    public ByteBufferMessageSet messages;
    public PartitionTopicInfo topicInfo;
    public long fetchOffset;

    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchedDataChunk that = (FetchedDataChunk) o;

        if (fetchOffset != that.fetchOffset) return false;
        if (messages != null ? !messages.equals(that.messages) : that.messages != null) return false;
        if (topicInfo != null ? !topicInfo.equals(that.topicInfo) : that.topicInfo != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = messages != null ? messages.hashCode() : 0;
        result = 31 * result + (topicInfo != null ? topicInfo.hashCode() : 0);
        result = 31 * result + (int) (fetchOffset ^ (fetchOffset >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "FetchedDataChunk{" +
                "messages=" + messages +
                ", topicInfo=" + topicInfo +
                ", fetchOffset=" + fetchOffset +
                '}';
    }
}
