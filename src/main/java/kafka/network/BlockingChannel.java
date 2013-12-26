package kafka.network;

import kafka.api.RequestOrResponse;
import kafka.common.KafkaException;
import kafka.utils.NonThreadSafe;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;

/**
 * A simple blocking channel with timeouts correctly enabled.
 */
@NonThreadSafe
public class BlockingChannel {
    public static final int UseDefaultBufferSize = -1;


    public String host;
    public int port;
    public int readBufferSize;
    public int writeBufferSize;
    public int readTimeoutMs;

    public BlockingChannel(String host, int port, int readBufferSize, int writeBufferSize, int readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
    }

    private boolean connected = false;
    private SocketChannel channel = null;
    private ReadableByteChannel readChannel = null;
    private GatheringByteChannel writeChannel = null;
    private Object lock = new Object();

    Logger logger = LoggerFactory.getLogger(BlockingChannel.class);

    public void connect() {
        synchronized (lock) {
            if (connected) return;

            try {
                channel = SocketChannel.open();
                if (readBufferSize > 0)
                    channel.socket().setReceiveBufferSize(readBufferSize);
                if (writeBufferSize > 0)
                    channel.socket().setSendBufferSize(writeBufferSize);
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(readTimeoutMs);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                channel.connect(new InetSocketAddress(host, port));

                writeChannel = channel;
                readChannel = Channels.newChannel(channel.socket().getInputStream());
                connected = true;
                // settings may not match what we requested above
                String msg = "Created socket with SO_TIMEOUT = {} (requested {}), SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {}).";
                logger.debug(msg, channel.socket().getSoTimeout(),
                        readTimeoutMs,
                        channel.socket().getReceiveBufferSize(),
                        readBufferSize,
                        channel.socket().getSendBufferSize(),
                        writeBufferSize);
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }
    }

    public void disconnect() {
        synchronized (lock) {
            if (connected || channel != null) {
                // closing the main socket channel *should* close the read channel
                // but let's do it to be sure.
                Utils.closeQuietly(channel);
                Utils.closeQuietly(channel.socket());
                Utils.closeQuietly(readChannel);
                channel = null;
                readChannel = null;
                writeChannel = null;
                connected = false;
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public int send(RequestOrResponse request) {
        if (!connected)
            throw new KafkaException("ClosedChannelException");

        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
        return send.writeCompletely(writeChannel);
    }

    public Receive receive() {
        if (!connected)
            throw new KafkaException("ClosedChannelException");

        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(readChannel);

        return response;
    }
}
