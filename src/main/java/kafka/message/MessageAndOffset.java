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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageAndOffset that = (MessageAndOffset) o;

        if (offset != that.offset) return false;
        if (!message.equals(that.message)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = message.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }
}
