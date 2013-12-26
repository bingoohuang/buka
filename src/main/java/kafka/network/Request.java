package kafka.network;

import com.google.common.collect.Lists;
import kafka.api.FetchRequest;
import kafka.api.RequestKeys;
import kafka.api.RequestOrResponse;
import kafka.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import static java.lang.Math.max;

public class Request {
    public final int processor;
    public final Object requestKey;
    private ByteBuffer buffer;
    public final long startTimeMs;
    public final SocketAddress remoteAddress /*= new InetSocketAddress(0)*/;

    public Request(int processor,
                   Object requestKey,
                   ByteBuffer buffer,
                   long startTimeMs) {
        this(processor, requestKey, buffer, startTimeMs, new InetSocketAddress(0));
    }

    public Request(int processor,
                   Object requestKey,
                   ByteBuffer buffer,
                   long startTimeMs,
                   SocketAddress remoteAddress) {
        this.processor = processor;
        this.requestKey = requestKey;
        this.buffer = buffer;
        this.startTimeMs = startTimeMs;
        this.remoteAddress = remoteAddress;

        init();
    }

    volatile long requestDequeueTimeMs = -1L;
    volatile long apiLocalCompleteTimeMs = -1L;
    volatile long responseCompleteTimeMs = -1L;
    volatile long responseDequeueTimeMs = -1L;

    short requestId;
    public RequestOrResponse requestObj;
    private Logger requestLogger;
    Logger logger = LoggerFactory.getLogger(Request.class);

    private void init() {
        requestId = buffer.getShort();
        requestObj = RequestKeys.deserializerForKey(requestId).readFrom(buffer);
        buffer = null;

        requestLogger = LoggerFactory.getLogger("kafka.request.logger");

        logger.trace("Processor {} received request : {}", processor, requestObj);
    }

    public void updateRequestMetrics() {
        long endTimeMs = SystemTime.instance.milliseconds();
        // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes since the remote
        // processing time is really small. In this case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
        if (apiLocalCompleteTimeMs < 0)
            apiLocalCompleteTimeMs = responseCompleteTimeMs;
        long requestQueueTime = max(requestDequeueTimeMs - startTimeMs, 0L);
        long apiLocalTime = max(apiLocalCompleteTimeMs - requestDequeueTimeMs, 0L);
        long apiRemoteTime = max(responseCompleteTimeMs - apiLocalCompleteTimeMs, 0L);
        long responseQueueTime = max(responseDequeueTimeMs - responseCompleteTimeMs, 0L);
        long responseSendTime = max(endTimeMs - responseDequeueTimeMs, 0L);
        long totalTime = endTimeMs - startTimeMs;

        String key = RequestKeys.nameForKey(requestId);
        RequestMetrics requestMetrics = RequestMetrics.metricsMap.get(key);
        List<RequestMetrics> metricsList = Lists.newArrayList(requestMetrics);
        if (requestId == RequestKeys.FetchKey) {
            boolean isFromFollower = ((FetchRequest) requestObj).isFromFollower();
            metricsList.add(isFromFollower
                    ? RequestMetrics.metricsMap.get(RequestMetrics.followFetchMetricName)
                    : RequestMetrics.metricsMap.get(RequestMetrics.consumerFetchMetricName));
        }

        for (RequestMetrics m : metricsList) {
            m.requestRate.mark();
            m.requestQueueTimeHist.update(requestQueueTime);
            m.localTimeHist.update(apiLocalTime);
            m.remoteTimeHist.update(apiRemoteTime);
            m.responseQueueTimeHist.update(responseQueueTime);
            m.responseSendTimeHist.update(responseSendTime);
            m.totalTimeHist.update(totalTime);
        }
        requestLogger.trace("Completed request:{} from client {};totalTime:{},requestQueueTime:{},localTime:{},remoteTime:{},responseQueueTime:{},sendTime:{}",
                requestObj, remoteAddress, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime);
    }
}
