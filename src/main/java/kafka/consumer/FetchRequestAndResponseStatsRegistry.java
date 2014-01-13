package kafka.consumer;

import com.google.common.base.Function;
import com.yammer.metrics.core.Histogram;
import kafka.common.ClientIdAndBroker;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;
import kafka.utils.Pool;

import java.util.concurrent.TimeUnit;

/**
 * Stores the fetch request and response stats information of each consumer client in a (clientId -> FetchRequestAndResponseStats) map.
 */
public class FetchRequestAndResponseStatsRegistry {
    private static Function<String, FetchRequestAndResponseStats> valueFactory = new Function<String, FetchRequestAndResponseStatsRegistry.FetchRequestAndResponseStats>() {
        @Override
        public FetchRequestAndResponseStatsRegistry.FetchRequestAndResponseStats apply(String k) {
            return new FetchRequestAndResponseStats(k);
        }
    };
    private static Pool<String, FetchRequestAndResponseStats> globalStats = new Pool<String, FetchRequestAndResponseStats>(valueFactory);

    public static FetchRequestAndResponseStats getFetchRequestAndResponseStats(String clientId) {
        return globalStats.getAndMaybePut(clientId);
    }


    public static class FetchRequestAndResponseStats {
        public String clientId;

        /**
         * Tracks metrics of the requests made by a given consumer client to all brokers, and the responses obtained from the brokers.
         *
         * @param clientId ClientId of the given consumer
         */
        public FetchRequestAndResponseStats(String clientId) {
            this.clientId = clientId;
            allBrokersStats = new FetchRequestAndResponseMetrics(new ClientIdAndBroker(clientId, "AllBrokers"));
        }

        private Function<ClientIdAndBroker, FetchRequestAndResponseMetrics> valueFactory = new Function<ClientIdAndBroker, FetchRequestAndResponseMetrics>() {
            @Override
            public FetchRequestAndResponseMetrics apply(ClientIdAndBroker k) {
                return new FetchRequestAndResponseMetrics(k);
            }
        };

        private Pool<ClientIdAndBroker, FetchRequestAndResponseMetrics> stats = new Pool<ClientIdAndBroker, FetchRequestAndResponseMetrics>(valueFactory);
        private FetchRequestAndResponseMetrics allBrokersStats;

        public FetchRequestAndResponseMetrics getFetchRequestAndResponseAllBrokersStats() {
            return allBrokersStats;
        }

        public FetchRequestAndResponseMetrics getFetchRequestAndResponseStats(String brokerInfo) {
            return stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerInfo + "-"));
        }
    }

    public static class FetchRequestAndResponseMetrics extends KafkaMetricsGroup {
        public ClientIdAndBroker metricId;

        public FetchRequestAndResponseMetrics(ClientIdAndBroker metricId) {
            this.metricId = metricId;
            requestTimer = new KafkaTimer(newTimer(metricId + "FetchRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
            requestSizeHist = newHistogram(metricId + "FetchResponseSize");
        }

        public KafkaTimer requestTimer;
        public Histogram requestSizeHist;
    }
}
