package kafka.network;

import kafka.utils.Utils;

import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * A set of composite sends, sent one after another
 */
public abstract class MultiSend<S extends Send> extends Send {
    public final List<S> sends;

    protected MultiSend(List<S> sends) {
        this.sends = sends;
        current = sends;
    }

    public int expectedBytesToWrite;
    private List<S> current;
    public int totalWritten = 0;

    /**
     * This method continues to write to the socket buffer till an incomplete
     * write happens. On an incomplete write, it returns to the caller to give it
     * a chance to schedule other work till the buffered write completes.
     */
    public int writeTo(GatheringByteChannel channel) {
        expectIncomplete();
        int totalWrittenPerCall = 0;
        boolean sendComplete = false;
        do {
            int written = Utils.head(current).writeTo(channel);
            totalWritten += written;
            totalWrittenPerCall += written;
            sendComplete = Utils.head(current).complete();
            if (sendComplete)
                current = Utils.tail(current);
        } while (!complete() && sendComplete);
        logger.trace("Bytes written as part of multisend call : {} Total bytes written so far : Expected bytes to write : {}",
                totalWrittenPerCall, totalWritten, expectedBytesToWrite);

        return totalWrittenPerCall;
    }

    public boolean complete() {
        if (current == null) {
            if (totalWritten != expectedBytesToWrite)
                logger.error("mismatch in sending bytes over socket; expected: {} actual: {}", expectedBytesToWrite, totalWritten);
            return true;
        } else {
            return false;
        }
    }
}
