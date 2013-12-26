package kafka.controller;

import kafka.api.*;
import kafka.network.BlockingChannel;
import kafka.network.Receive;
import kafka.utils.Callable1;
import kafka.utils.ShutdownableThread;
import kafka.utils.Tuple2;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class RequestSendThread extends ShutdownableThread {
    public int controllerId;
    public ControllerContext controllerContext;
    public int toBrokerId;
    public BlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>> queue;
    public BlockingChannel channel;

    public RequestSendThread(int controllerId,
                             ControllerContext controllerContext,
                             int toBrokerId,
                             BlockingQueue<Tuple2<RequestOrResponse, Callable1<RequestOrResponse>>> queue,
                             BlockingChannel channel) {
        super(String.format("Controller-%d-to-broker-%d-send-thread", controllerId, toBrokerId));

        this.controllerId = controllerId;
        this.controllerContext = controllerContext;
        this.toBrokerId = toBrokerId;
        this.queue = queue;
        this.channel = channel;
    }

    private Object lock = new Object();
    private Logger stateChangeLogger = LoggerFactory.getLogger(KafkaControllers.stateChangeLogger);

    Logger logger = LoggerFactory.getLogger(RequestSendThread.class);

    @Override
    public void doWork() {
        Tuple2<RequestOrResponse, Callable1<RequestOrResponse>> queueItem = Utils.take(queue);
        RequestOrResponse request = queueItem._1;
        Callable1<RequestOrResponse> callback = queueItem._2;

        Receive receive = null;

        try {
            synchronized (lock) {
                channel.connect(); // establish a socket connection if needed
                channel.send(request);
                receive = channel.receive();
                RequestOrResponse response = null;
                switch (request.requestId) {
                    case RequestKeys.LeaderAndIsrKey:
                        response = LeaderAndIsrResponse.readFrom(receive.buffer());
                        break;
                    case RequestKeys.StopReplicaKey:
                        response = StopReplicaResponse.readFrom(receive.buffer());
                        break;
                    case RequestKeys.UpdateMetadataKey:
                        response = UpdateMetadataResponse.readFrom(receive.buffer());
                }
                stateChangeLogger.trace("Controller {} epoch {} received response correlationId {} for a request sent to broker {}",
                        controllerId, controllerContext.epoch, response.correlationId, toBrokerId);

                if (callback != null) {
                    callback.apply(response);
                }
            }
        } catch (Throwable e) {
            logger.warn("Controller {} fails to send a request to broker {}", controllerId, toBrokerId, e);
            // If there is any socket error (eg, socket timeout), the channel is no longer usable and needs to be recreated.
            channel.disconnect();
        }
    }
}
