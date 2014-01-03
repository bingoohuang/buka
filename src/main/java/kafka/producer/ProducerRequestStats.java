package kafka.producer;

import com.google.common.base.Function;
import com.yammer.metrics.core.Histogram;
import kafka.common.ClientIdAndBroker;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;
import kafka.utils.Pool;

import java.util.concurrent.TimeUnit;

public class ProducerRequestStats {
    public static class ProducerRequestMetrics extends KafkaMetricsGroup {
        public ClientIdAndBroker metricId;

        public ProducerRequestMetrics(ClientIdAndBroker metricId) {
            this.metricId = metricId;
            requestTimer = new KafkaTimer(newTimer(metricId + "ProducerRequestRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
            requestSizeHist = newHistogram(metricId + "ProducerRequestSize");
        }

        public KafkaTimer requestTimer;
        public Histogram requestSizeHist;

    }

    public String clientId;

    /**
     * Tracks metrics of requests made by a given producer client to all brokers.
     *
     * @param clientId ClientId of the given producer
     */
    public ProducerRequestStats(String clientId) {
        this.clientId = clientId;
        valueFactory = new Function<ClientIdAndBroker, ProducerRequestStats.ProducerRequestMetrics>() {
            @Override
            public ProducerRequestStats.ProducerRequestMetrics apply(ClientIdAndBroker k) {
                return new ProducerRequestMetrics(k);
            }
        };

        stats = new Pool<ClientIdAndBroker, ProducerRequestMetrics>(valueFactory);
        allBrokersStats = new ProducerRequestMetrics(new ClientIdAndBroker(clientId, "AllBrokers"));
    }

    private Function<ClientIdAndBroker, ProducerRequestMetrics> valueFactory;
    private Pool<ClientIdAndBroker, ProducerRequestMetrics> stats;
    private ProducerRequestMetrics allBrokersStats;

    public ProducerRequestMetrics getProducerRequestAllBrokersStats() {
        return allBrokersStats;
    }

    public ProducerRequestMetrics getProducerRequestStats(String brokerInfo) {
        return stats.getAndMaybePut(new ClientIdAndBroker(clientId, brokerInfo + "-"));
    }

    /**
     * Stores the request stats information of each producer client in a (clientId -> ProducerRequestStats) map.
     */
    public static class ProducerRequestStatsRegistry {
        private static Function<String, ProducerRequestStats> valueFactory = new Function<String, kafka.producer.ProducerRequestStats>() {
            @Override
            public ProducerRequestStats apply(String k) {
                return new ProducerRequestStats(k);
            }
        };
        private static Pool<String, ProducerRequestStats> globalStats = new Pool<String, ProducerRequestStats>(valueFactory);

        public static ProducerRequestStats getProducerRequestStats(String clientId) {
            return globalStats.getAndMaybePut(clientId);
        }
    }
}
