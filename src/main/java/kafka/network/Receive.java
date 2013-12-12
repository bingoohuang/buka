package kafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A transmission that is being received from a channel
 */
public abstract class Receive extends Transmission {
    Logger logger = LoggerFactory.getLogger(Receive.class);

    public abstract ByteBuffer buffer();

    public abstract int readFrom(ReadableByteChannel channel);


    public int readCompletely(ReadableByteChannel channel) {
        int totalRead = 0;
        while (!complete()) {
            int read = readFrom(channel);
            logger.trace("{} bytes read.", read);
            totalRead += read;
        }
        return totalRead;
    }
}
