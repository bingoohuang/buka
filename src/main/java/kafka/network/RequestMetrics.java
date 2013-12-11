package kafka.network;

import com.google.common.collect.Maps;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import kafka.api.RequestKeys;
import kafka.api.RequestReader;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.Tuple2;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RequestMetrics extends KafkaMetricsGroup {
    public static final Map<String, RequestMetrics> metricsMap = Maps.newHashMap();
    public static final String consumerFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "-Consumer";
    public static final String followFetchMetricName = RequestKeys.nameForKey(RequestKeys.FetchKey) + "-Follower";

    static {
        for (Tuple2<String, RequestReader> tuple2 : RequestKeys.keyToNameAndDeserializerMap.values()) {
            metricsMap.put(tuple2._1, new RequestMetrics(tuple2._1));
        }

        metricsMap.put(consumerFetchMetricName, new RequestMetrics(consumerFetchMetricName));
        metricsMap.put(followFetchMetricName, new RequestMetrics(followFetchMetricName));
    }

    public final String name;

    public RequestMetrics(String name) {
        this.name = name;
        requestRate = newMeter(name + "-RequestsPerSec", "requests", TimeUnit.SECONDS);
        requestQueueTimeHist = newHistogram(name + "-RequestQueueTimeMs");
        localTimeHist = newHistogram(name + "-LocalTimeMs");
        remoteTimeHist = newHistogram(name + "-RemoteTimeMs");
        responseQueueTimeHist = newHistogram(name + "-ResponseQueueTimeMs");
        responseSendTimeHist = newHistogram(name + "-ResponseSendTimeMs");
        totalTimeHist = newHistogram(name + "-TotalTimeMs");
    }

    public Meter requestRate;
    // time a request spent in a request queue
    public Histogram requestQueueTimeHist;
    // time a request takes to be processed at the local broker
    public Histogram localTimeHist;
    // time a request takes to wait on remote brokers (only relevant to fetch and produce requests)
    public Histogram remoteTimeHist;
    // time a response spent in a response queue
    public Histogram responseQueueTimeHist;
    // time to send the response to the requester
    public Histogram responseSendTimeHist;
    public Histogram totalTimeHist;
}
