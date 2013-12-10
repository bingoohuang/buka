package kafka.message;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Message set helper functions
 */
public abstract class MessageSets {
    public static final int MessageSizeLength = 4;
    public static final int OffsetLength = 8;
    public static final int LogOverhead = MessageSizeLength + OffsetLength;
    public static final ByteBufferMessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    /**
     * The size of a message set containing the given messages
     */
    public static int messageSetSize(Iterable<Message> messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }

        return size;
    }

    /**
     * The size of a list of messages
     */
    public static int messageSetSize(List<Message> messages) {
        int size = 0;
        Iterator<Message> iter = messages.iterator();
        while (iter.hasNext()) {
            Message message = iter.next();
            size += entrySize(message);
        }

        return size;
    }

    /**
     * The size of a size-delimited entry in a message set
     */
    public static int entrySize(Message message) {
        return LogOverhead + message.size();
    }
}
