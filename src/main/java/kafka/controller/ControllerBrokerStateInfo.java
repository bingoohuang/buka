package kafka.controller;

import kafka.api.RequestOrResponse;
import kafka.cluster.Broker;
import kafka.network.BlockingChannel;
import kafka.utils.Callable1;
import kafka.utils.Tuple2;

import java.util.concurrent.BlockingQueue;

public class ControllerBrokerStateInfo {
    public BlockingChannel channel;
    public Broker broker;
    public BlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>> messageQueue;
    public RequestSendThread requestSendThread;

    public ControllerBrokerStateInfo(BlockingChannel channel,
                                     Broker broker,
                                     BlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>> messageQueue,
                                     RequestSendThread requestSendThread) {
        this.channel = channel;
        this.broker = broker;
        this.messageQueue = messageQueue;
        this.requestSendThread = requestSendThread;
    }

}
