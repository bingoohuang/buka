package kafka.network;

import kafka.common.KafkaException;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * Thread that accepts and configures new connections. There is only need for one of these
 */
public class Acceptor extends AbstractServerThread {
    public String host;
    public int port;
    private Processor[] processors;
    public int sendBufferSize;
    public int recvBufferSize;

    public Acceptor(String host, int port, Processor[] processors, int sendBufferSize, int recvBufferSize) {
        this.host = host;
        this.port = port;
        this.processors = processors;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;

        serverChannel = openServerSocket(host, port);
    }

    Logger logger = LoggerFactory.getLogger(Acceptor.class);


    public ServerSocketChannel serverChannel;

    /**
     * Accept loop that checks for new connection attempts
     */
    @Override
    public void run() {
        try {
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            throw new KafkaException(e);
        }

        startupComplete();
        int currentProcessor = 0;
        while (isRunning()) {
            int ready = Utils.select(selector, 500);
            if (ready <= 0) continue;

            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iter = keys.iterator();
            while (iter.hasNext() && isRunning()) {
                SelectionKey key;
                try {
                    key = iter.next();
                    iter.remove();
                    if (key.isAcceptable())
                        accept(key, processors[currentProcessor]);
                    else
                        throw new IllegalStateException("Unrecognized key state for acceptor thread.");

                    // round robin to the next processor thread
                    currentProcessor = (currentProcessor + 1) % processors.length;
                } catch (Throwable e) {
                    logger.error("Error in acceptor", e);
                }
            }
        }
        logger.debug("Closing server socket and selector.");
        Utils.closeQuietly(serverChannel);
        Utils.closeQuietly(selector);
        shutdownComplete();
    }


    /*
     * Create a server socket to listen for connections on.
     */
    private ServerSocketChannel openServerSocket(String host, int port) {
        InetSocketAddress socketAddress = null;
        if (host == null || host.trim().isEmpty())
            socketAddress = new InetSocketAddress(port);
        else
            socketAddress = new InetSocketAddress(host, port);

        ServerSocketChannel serverChannel = null;
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            try {
                serverChannel.socket().bind(socketAddress);
                logger.info("Awaiting socket connections on {}:{}.", socketAddress.getHostName(), port);
            } catch (SocketException e) {
                throw new KafkaException("Socket server failed to bind to %s:%d: %s.", socketAddress.getHostName(), port, e.getMessage(), e);
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        return serverChannel;
    }

    /*
     * Accept a new connection
     */
    public Selector accept(SelectionKey key, Processor processor) {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = null;
        try {
            serverSocketChannel.socket().setReceiveBufferSize(recvBufferSize);

            socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSendBufferSize(sendBufferSize);

            logger.debug("Accepted connection from {} on {}. sendBufferSize [actual|requested]: [{}|{}] recvBufferSize [actual|requested]: [{}|{}]",
                    socketChannel.socket().getInetAddress(), socketChannel.socket().getLocalSocketAddress(),
                    socketChannel.socket().getSendBufferSize(), sendBufferSize,
                    socketChannel.socket().getReceiveBufferSize(), recvBufferSize);
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        return processor.accept(socketChannel);
    }
}
