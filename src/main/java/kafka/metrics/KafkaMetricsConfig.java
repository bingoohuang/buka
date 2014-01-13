package kafka.metrics;

import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.List;

public class KafkaMetricsConfig {
    public VerifiableProperties props;

    public KafkaMetricsConfig(VerifiableProperties props) {
        this.props = props;

        reporters = Utils.parseCsvList(props.getString("kafka.metrics.reporters", ""));
        pollingIntervalSecs = props.getInt("kafka.metrics.polling.interval.secs", 10);
    }

    /**
     * Comma-separated list of reporter types. These classes should be on the
     * classpath and will be instantiated at run-time.
     */
    public List<String> reporters;

    /**
     * The metrics polling interval (in seconds).
     */
    public int pollingIntervalSecs;
}
