package kafka.message;

public class MessageAndOffset {
    public final Message message;
    public final long offset;

    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    /**
     * Compute the offset of the next message in the log
     */
    public long nextOffset() {
        return offset + 1;
    }

    @Override
    public String toString() {
        return "MessageAndOffset{" +
                "message=" + message +
                ", offset=" + offset +
                '}';
    }
}
