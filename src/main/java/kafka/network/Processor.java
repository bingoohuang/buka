package kafka.network;

import kafka.common.KafkaException;
import kafka.utils.SystemTime;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
public class Processor extends AbstractServerThread {
    public int id;
    public Time time;
    public int maxRequestSize;
    public RequestChannel requestChannel;

    public Processor(int id, Time time, int maxRequestSize, RequestChannel requestChannel) {
        this.id = id;
        this.time = time;
        this.maxRequestSize = maxRequestSize;
        this.requestChannel = requestChannel;
    }

    private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<SocketChannel>();

    Logger logger = LoggerFactory.getLogger(Processor.class);

    @Override
    public void run() {
        startupComplete();
        while (isRunning()) {
            // setup any new connections that have been queued up
            configureNewConnections();
            // register any new responses for writing
            processNewResponses();
            long startSelectTime = SystemTime.instance.milliseconds();
            int ready = Utils.select(selector, 300);
            logger.trace("Processor id " + id + " selection time = " + (SystemTime.instance.milliseconds() - startSelectTime) + " ms");
            if (ready > 0) {
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iter = keys.iterator();
                while (iter.hasNext() && isRunning()) {
                    SelectionKey key = null;
                    try {
                        key = iter.next();
                        iter.remove();
                        if (key.isReadable())
                            read(key);
                        else if (key.isWritable())
                            write(key);
                        else if (!key.isValid())
                            close(key);
                        else
                            throw new IllegalStateException("Unrecognized key state for processor thread.");

                    } catch (InvalidRequestException e) {
                        logger.info("Closing socket connection to {} due to invalid request: {}", channelFor(key).socket().getInetAddress(), e.getMessage());
                        close(key);
                    } catch (Throwable e) {
                        logger.error("Closing socket for {} because of error", channelFor(key).socket().getInetAddress(), e);
                        close(key);
                    }
                }
            }
        }

        logger.debug("Closing selector.");

        Utils.closeQuietly(selector);
        shutdownComplete();
    }

    private void processNewResponses() {
        Response curr = requestChannel.receiveResponse(id);
        while (curr != null) {
            SelectionKey key = (SelectionKey) curr.request.requestKey;
            try {
                switch (curr.responseAction) {
                    case NoOpAction:
                        // There is no response to send to the client, we need to read more pipelined requests
                        // that are sitting in the server's socket buffer
                        curr.request.updateRequestMetrics();
                        logger.trace("Socket server received empty response to send, registering for read: {}", curr);
                        key.interestOps(SelectionKey.OP_READ);
                        key.attach(null);

                        break;
                    case SendAction:
                        logger.trace("Socket server received response to send, registering for write: {}", curr);
                        key.interestOps(SelectionKey.OP_WRITE);
                        key.attach(curr);
                        break;

                    case CloseConnectionAction:
                        curr.request.updateRequestMetrics();
                        logger.trace("Closing socket connection actively according to the response code.");
                        close(key);
                        break;

                    default:
                        throw new KafkaException("No mapping found for response code ");
                }
            } catch (CancelledKeyException e) {
                logger.debug("Ignoring response for closed socket.");
                close(key);
            } finally {
                curr = requestChannel.receiveResponse(id);
            }
        }
    }

    private void close(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        logger.debug("Closing connection from {}", channel.socket().getRemoteSocketAddress());
        Utils.closeQuietly(channel.socket());
        Utils.closeQuietly(channel);
        key.attach(null);
        key.cancel();
    }

    /**
     * Queue up a new connection for reading
     */
    public Selector accept(SocketChannel socketChannel) {
        newConnections.add(socketChannel);
        return wakeup();
    }

    /**
     * Register any new connections that have been queued up
     */
    private void configureNewConnections() {
        try {
            while (newConnections.size() > 0) {
                SocketChannel channel = newConnections.poll();
                logger.debug("Processor {} listening to new connection from {}", id, channel.socket().getRemoteSocketAddress());
                channel.register(selector, SelectionKey.OP_READ);
            }
        } catch (ClosedChannelException e) {
            throw new KafkaException(e);
        }
    }

    /*
     * Process reads from ready sockets
     */
    public void read(SelectionKey key) {
        SocketChannel socketChannel = channelFor(key);
        Receive receive = (Receive) key.attachment();
        if (key.attachment() == null) {
            receive = new BoundedByteBufferReceive(maxRequestSize);
            key.attach(receive);
        }
        int read = receive.readFrom(socketChannel);
        SocketAddress address = socketChannel.socket().getRemoteSocketAddress();
        logger.trace("{} bytes read from {}", read, address);
        if (read < 0) {
            close(key);
        } else if (receive.complete()) {
            Request req = new Request(/*processor =*/ id,/* requestKey =*/ key, /*buffer = */receive.buffer(),
                      /*startTimeMs =*/ time.milliseconds(), /*remoteAddress = */address);
            requestChannel.sendRequest(req);
            key.attach(null);
            // explicitly reset interest ops to not READ, no need to wake up the selector just yet
            key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));
        } else {
            // more reading to be done
            logger.trace("Did not finish reading, registering for read again on connection {}", socketChannel.socket().getRemoteSocketAddress());
            key.interestOps(SelectionKey.OP_READ);
            wakeup();
        }
    }

    /*
     * Process writes to ready sockets
     */
    public void write(SelectionKey key) {
        SocketChannel socketChannel = channelFor(key);
        Response response = (Response) key.attachment();
        Send responseSend = response.responseSend;
        if (responseSend == null)
            throw new IllegalStateException("Registered for write interest but no response attached to key.");
        int written = responseSend.writeTo(socketChannel);
        logger.trace("{} bytes written to {} using key {}", written, socketChannel.socket().getRemoteSocketAddress(), key);
        if (responseSend.complete()) {
            response.request.updateRequestMetrics();
            key.attach(null);
            logger.trace("Finished writing, registering for read on connection {}", socketChannel.socket().getRemoteSocketAddress());
            key.interestOps(SelectionKey.OP_READ);
        } else {
            logger.trace("Did not finish writing, registering for write again on connection {}", socketChannel.socket().getRemoteSocketAddress());
            key.interestOps(SelectionKey.OP_WRITE);
            wakeup();
        }
    }

    private SocketChannel channelFor(SelectionKey key) {
        return (SocketChannel) key.channel();
    }
}
