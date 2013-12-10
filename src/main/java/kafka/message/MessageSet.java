package kafka.message;

import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;

/**
 * A set of messages with offsets. A message set has a fixed serialized form, though the container
 * for the bytes could be either in-memory or on disk. The format of each message is
 * as follows:
 * 8 byte message offset number
 * 4 byte size containing an integer N
 * N message bytes as described in the Message class
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {

    /**
     * Write the messages in this set to the given channel starting at the given offset byte.
     * Less than the complete amount may be written, but no more than maxSize can be. The number
     * of bytes written is returned
     */
    public abstract int writeTo(GatheringByteChannel channel, long offset, int maxSize);

    /**
     * Provides an iterator over the message/offset pairs in this set
     */
    public abstract Iterator<MessageAndOffset> iterator();

    /**
     * Gives the total size of this message set in bytes
     */
    public abstract int sizeInBytes();

    /**
     * Validate the checksum of all the messages in the set. Throws an InvalidMessageException if the checksum doesn't
     * match the payload for any message.
     */
    public void validate() {
        for (MessageAndOffset messageAndOffset : this)
            if (!messageAndOffset.message.isValid())
                throw new InvalidMessageException();
    }

    /**
     * Print this message set's contents. If the message set has more than 100 messages, just
     * print the first 100.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName() + "(");
        Iterator<MessageAndOffset> iter = this.iterator();
        int i = 0;
        while (iter.hasNext() && i < 100) {
            MessageAndOffset message = iter.next();
            builder.append(message);
            if (iter.hasNext())
                builder.append(", ");
            i += 1;
        }
        if (iter.hasNext())
            builder.append("...");
        builder.append(")");
        return builder.toString();
    }

}