package kafka.controller;

import com.google.common.collect.Maps;
import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.network.BlockingChannel;
import kafka.server.KafkaConfig;
import kafka.utils.Callable1;
import kafka.utils.Callable2;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ControllerChannelManager {
    private ControllerContext controllerContext;
    public KafkaConfig config;

    public ControllerChannelManager(ControllerContext controllerContext, KafkaConfig config) {
        this.controllerContext = controllerContext;
        this.config = config;

        logger = LoggerFactory.getLogger(ControllerChannelManager.class
                + "[Channel manager on controller " + config.brokerId + "]: ");
        Utils.foreach(controllerContext.liveBrokers(), new Callable1<Broker>() {
            @Override
            public void apply(Broker _) {
                addNewBroker(_);
            }
        });
    }

    private HashMap<Integer, ControllerBrokerStateInfo> brokerStateInfo = Maps.newHashMap();
    private Object brokerLock = new Object();
    Logger logger;


    public void startup() {
        synchronized (brokerLock) {
            Utils.foreach(brokerStateInfo, new Callable2<Integer, ControllerBrokerStateInfo>() {
                @Override
                public void apply(Integer brokerId, ControllerBrokerStateInfo brokerState) {
                    startRequestSendThread(brokerId);
                }
            });
        }
    }

    public void shutdown() {
        synchronized (brokerLock) {
            Utils.foreach(brokerStateInfo, new Callable2<Integer, ControllerBrokerStateInfo>() {
                @Override
                public void apply(Integer brokerId, ControllerBrokerStateInfo controllerBrokerStateInfo) {
                    removeExistingBroker(brokerId);
                }
            });
        }
    }

    public void sendRequest(int brokerId, RequestOrResponse request) {
        sendRequest(brokerId, request, null);
    }

    public void sendRequest(int brokerId, RequestOrResponse request, Callable1<RequestOrResponse> callback) {
        synchronized (brokerLock) {
            ControllerBrokerStateInfo stateInfo = brokerStateInfo.get(brokerId);
            if (stateInfo != null) {
                Utils.put(stateInfo.messageQueue, Tuple2.make(request, callback));
            } else {
                logger.warn("Not sending request {} to broker {}, since it is offline.", request, brokerId);
            }
        }
    }

    public void addBroker(Broker broker) {
        // be careful here. Maybe the startup() API has already started the request send thread
        synchronized (brokerLock) {
            if (!brokerStateInfo.containsKey(broker.id)) {
                addNewBroker(broker);
                startRequestSendThread(broker.id);
            }
        }
    }

    public void removeBroker(int brokerId) {
        synchronized (brokerLock) {
            removeExistingBroker(brokerId);
        }
    }

    private void addNewBroker(Broker broker) {
        LinkedBlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>>
                messageQueue = new LinkedBlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>>(config.controllerMessageQueueSize);
        logger.debug("Controller {} trying to connect to broker {}", config.brokerId, broker.id);
        BlockingChannel channel = new BlockingChannel(broker.host, broker.port,
                BlockingChannel.UseDefaultBufferSize,
                BlockingChannel.UseDefaultBufferSize,
                config.controllerSocketTimeoutMs);
        channel.connect();
        RequestSendThread requestThread = new RequestSendThread(config.brokerId, controllerContext, broker.id, messageQueue, channel);
        requestThread.setDaemon(false);
        brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(channel, broker, messageQueue, requestThread));
    }

    private void removeExistingBroker(int brokerId) {
        try {
            brokerStateInfo.get(brokerId).channel.disconnect();
            brokerStateInfo.get(brokerId).messageQueue.clear();
            brokerStateInfo.get(brokerId).requestSendThread.shutdown();
            brokerStateInfo.remove(brokerId);
        } catch (Throwable e) {
            logger.error("Error while removing broker by the controller", e);
        }
    }

    private void startRequestSendThread(int brokerId) {
        RequestSendThread requestThread = brokerStateInfo.get(brokerId).requestSendThread;
        if (requestThread.getState() == Thread.State.NEW)
            requestThread.start();
    }
}
