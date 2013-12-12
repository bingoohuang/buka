package kafka.network;

import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import kafka.common.KafkaException;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.SystemTime;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestChannel extends KafkaMetricsGroup {
    public int numProcessors;
    public int queueSize;

    public RequestChannel(int numProcessors, int queueSize) {
        this.numProcessors = numProcessors;
        this.queueSize = queueSize;

        requestQueue = new ArrayBlockingQueue<Request>(queueSize);
        responseQueues = (BlockingQueue<Response>[]) new Object[numProcessors];
        for (int i = 0; i < numProcessors; ++i)
            responseQueues[i] = new LinkedBlockingQueue<Response>();

        newGauge("RequestQueueSize",
                new Gauge<Integer>() {
                    @Override
                    public Integer value() {
                        return requestQueue.size();
                    }
                });

        for (int i = 0; i < numProcessors; ++i) {
            final int finalI = i;
            newGauge(
                    "Processor-" + i + "-ResponseQueueSize",
                    new Gauge<Integer>() {
                        @Override
                        public Integer value() {
                            return responseQueues[finalI].size();
                        }
                    });
        }
    }

    private List<ResponseListener> responseListeners = Lists.newArrayList();
    private ArrayBlockingQueue<Request> requestQueue;
    private BlockingQueue<Response>[] responseQueues;


    /**
     * Send a request to be handled, potentially blocking until there is room in the queue for the request
     */
    public void sendRequest(Request request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Send a response back to the socket server to be sent over the network
     */
    public void sendResponse(Response response) {
        try {
            responseQueues[response.processor].put(response);
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }

        for (ResponseListener onResponse : responseListeners)
            onResponse.onResponse(response.processor);
    }

    /**
     * No operation to take for the request, need to read more over the network
     */
    public void noOperation(int processor, Request request) {
        try {
            responseQueues[processor].put(new Response(processor, request, null, ResponseAction.NoOpAction));
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
        for (ResponseListener onResponse : responseListeners)
            onResponse.onResponse(processor);
    }

    /**
     * Close the connection for the request
     */
    public void closeConnection(int processor, Request request) {
        try {
            responseQueues[processor].put(new Response(processor, request, null, ResponseAction.CloseConnectionAction));
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
        for (ResponseListener onResponse : responseListeners)
            onResponse.onResponse(processor);
    }

    /**
     * Get the next request or block until there is one
     */
    public Request receiveRequest() {
        try {
            return requestQueue.take();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Get a response for the given processor if there is one
     */
    public Response receiveResponse(int processor) {
        Response response = responseQueues[processor].poll();
        if (response != null)
            response.request.responseDequeueTimeMs = SystemTime.instance.milliseconds();

        return response;
    }

    public void addResponseListener(ResponseListener onResponse) {
        responseListeners.add(onResponse);
    }

    public void shutdown() {
        requestQueue.clear();
    }
}
