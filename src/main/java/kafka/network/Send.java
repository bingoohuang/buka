package kafka.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.GatheringByteChannel;

/**
 * A transmission that is being sent out to the channel
 */
public abstract class Send extends Transmission {
    Logger logger = LoggerFactory.getLogger(Send.class);

    public abstract int writeTo(GatheringByteChannel channel);

    public int writeCompletely(GatheringByteChannel channel) {
        int totalWritten = 0;
        while (!complete()) {
            int written = writeTo(channel);
            logger.trace("{} bytes written.", written);
            totalWritten += written;
        }
        return totalWritten;
    }
}
