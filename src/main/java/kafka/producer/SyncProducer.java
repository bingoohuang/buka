package kafka.producer;

import kafka.api.*;
import kafka.metrics.KafkaTimer;
import kafka.network.BlockingChannel;
import kafka.network.BoundedByteBufferSend;
import kafka.network.Receive;
import kafka.utils.Function0;
import kafka.utils.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

@ThreadSafe
public class SyncProducer {
    public static short RequestKey = 0;
    public static Random randomGenerator = new Random();


    public SyncProducerConfig config;

    public SyncProducer(SyncProducerConfig config) {
        this.config = config;

        init();
    }

    private void init() {
        blockingChannel = new BlockingChannel(config.host, config.port, BlockingChannel.UseDefaultBufferSize,
                config.sendBufferBytes, config.requestTimeoutMs);
        brokerInfo = String.format("host_%s-port_%s", config.host, config.port);
        producerRequestStats = ProducerRequestStats.ProducerRequestStatsRegistry.getProducerRequestStats(config.clientId);
        logger.trace("Instantiating Scala Sync Producer");
    }

    Logger logger = LoggerFactory.getLogger(SyncProducer.class);

    private Object lock = new Object();
    volatile private boolean shutdown = false;
    private BlockingChannel blockingChannel;
    public String brokerInfo;
    public ProducerRequestStats producerRequestStats;


    private void verifyRequest(RequestOrResponse request) {
        /**
         * This seems a little convoluted, but the idea is to turn on verification simply changing log4j settings
         * Also, when verification is turned on, care should be taken to see that the logs don't fill up with unnecessary
         * data. So, leaving the rest of the logging at TRACE, while errors should be logged at ERROR level
         */
        if (logger.isDebugEnabled()) {
            ByteBuffer buffer = new BoundedByteBufferSend(request).buffer;
            logger.trace("verifying sendbuffer of size " + buffer.limit());
            short requestTypeId = buffer.getShort();
            if (requestTypeId == RequestKeys.ProduceKey) {
                RequestOrResponse request1 = ProducerRequestReader.instance.readFrom(buffer);
                logger.trace(request1.toString());
            }
        }
    }

    /**
     * Common functionality for the public send methods
     */
    private Receive doSend(RequestOrResponse request) {
        return doSend(request, true);
    }

    private Receive doSend(RequestOrResponse request, boolean readResponse) {
        synchronized (lock) {
            verifyRequest(request);
            getOrMakeConnection();

            Receive response = null;
            try {
                blockingChannel.send(request);
                if (readResponse)
                    response = blockingChannel.receive();
                else
                    logger.trace("Skipping reading response");
            } catch (Throwable e) {
                if (e.getCause() instanceof IOException) {
                    // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
                    disconnect();
                }
                throw e;
            }
            return response;
        }
    }

    /**
     * Send a message. If the producerRequest had required.request.acks=0, then the
     * returned response object is null
     */
    public ProducerResponse send(final ProducerRequest producerRequest) {
        int requestSize = producerRequest.sizeInBytes();
        producerRequestStats.getProducerRequestStats(brokerInfo).requestSizeHist.update(requestSize);
        producerRequestStats.getProducerRequestAllBrokersStats().requestSizeHist.update(requestSize);

        final Receive[] response = {null};
        final KafkaTimer specificTimer = producerRequestStats.getProducerRequestStats(brokerInfo).requestTimer;
        KafkaTimer aggregateTimer = producerRequestStats.getProducerRequestAllBrokersStats().requestTimer;
        aggregateTimer.time(new Function0<Object>() {
            @Override
            public Object apply() {
                specificTimer.time(new Function0<Object>() {
                    @Override
                    public Object apply() {
                        response[0] = doSend(producerRequest, producerRequest.requiredAcks != 0);
                        return null;
                    }
                });
                return null;
            }
        });

        if (producerRequest.requiredAcks != 0)
            return ProducerResponse.readFrom(response[0].buffer());
        else
            return null;
    }

    public TopicMetadataResponse send(TopicMetadataRequest request) {
        Receive response = doSend(request);
        return TopicMetadataResponse.readFrom(response.buffer());
    }

    public void close() {
        synchronized (lock) {
            disconnect();
            shutdown = true;
        }
    }

    private void reconnect() {
        disconnect();
        connect();
    }

    /**
     * Disconnect from current channel, closing connection.
     * Side effect: channel field is set to null on successful disconnect
     */
    private void disconnect() {
        try {
            if (blockingChannel.isConnected()) {
                logger.info("Disconnecting from " + config.host + ":" + config.port);
                blockingChannel.disconnect();
            }
        } catch (Exception e) {
            logger.error("Error on disconnect: ", e);
        }
    }

    private BlockingChannel connect() {
        if (!blockingChannel.isConnected() && !shutdown) {
            try {
                blockingChannel.connect();
                logger.info("Connected to " + config.host + ":" + config.port + " for producing");
            } catch (Exception e) {
                disconnect();
                logger.error("Producer connection to " + config.host + ":" + config.port + " unsuccessful", e);
                throw e;
            }
        }
        return blockingChannel;
    }

    private void getOrMakeConnection() {
        if (!blockingChannel.isConnected()) {
            connect();
        }
    }
}
