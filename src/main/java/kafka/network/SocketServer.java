package kafka.network;

import kafka.utils.SystemTime;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An NIO socket server. The threading model is
 * 1 Acceptor thread that handles new connections
 * N Processor threads that each have their own selector and read requests from sockets
 * M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
public class SocketServer {
    public int brokerId;
    public String host;
    public int port;
    public int numProcessorThreads;
    public int maxQueuedRequests;
    public int sendBufferSize;
    public int recvBufferSize;
    public int maxRequestSize;

    public SocketServer(int brokerId,
                        String host,
                        int port,
                        int numProcessorThreads,
                        int maxQueuedRequests,
                        int sendBufferSize,
                        int recvBufferSize) {
        this(brokerId, host, port, numProcessorThreads, maxQueuedRequests, sendBufferSize, recvBufferSize, Integer.MAX_VALUE);
    }

    public SocketServer(int brokerId,
                        String host,
                        int port,
                        int numProcessorThreads,
                        int maxQueuedRequests,
                        int sendBufferSize,
                        int recvBufferSize,
                        int maxRequestSize) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.maxQueuedRequests = maxQueuedRequests;
        this.sendBufferSize = sendBufferSize;
        this.recvBufferSize = recvBufferSize;
        this.maxRequestSize = maxRequestSize;

        logger = LoggerFactory.getLogger(SocketServer.class + "[on Broker " + brokerId + "]");
        processors = new Processor[numProcessorThreads];
        requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests);
    }

    Logger logger;
    private Time time = SystemTime.instance;
    private Processor[] processors;

    volatile private Acceptor acceptor = null;
    public RequestChannel requestChannel;

    /**
     * Start the socket server
     */
    public void startup() {
        for (int i = 0; i < numProcessorThreads; ++i) {
            processors[i] = new Processor(i, time, maxRequestSize, requestChannel);
            Utils.newThread(String.format("kafka-processor-%d-%d", port, i), processors[i], false).start();
        }
        // register the processor threads for notification of responses
        requestChannel.addResponseListener(new ResponseListener() {
            @Override
            public void onResponse(int id) {
                processors[id].wakeup();
            }
        });

        // start accepting connections
        this.acceptor = new Acceptor(host, port, processors, sendBufferSize, recvBufferSize);
        Utils.newThread("kafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
        logger.info("Started");
    }

    /**
     * Shutdown the socket server
     */
    public void shutdown() {
        logger.info("Shutting down");
        if (acceptor != null)
            acceptor.shutdown();
        for (Processor processor : processors)
            processor.shutdown();
        logger.info("Shutdown completed");
    }
}
