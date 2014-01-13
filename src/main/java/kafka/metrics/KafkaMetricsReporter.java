package kafka.metrics;

import kafka.utils.Callable1;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class KafkaMetricsReporter {
    public abstract void init(VerifiableProperties props);

    public static AtomicBoolean ReporterStarted = new AtomicBoolean(false);

    public static void startReporters(final VerifiableProperties verifiableProps) {
        synchronized (ReporterStarted) {
            if (ReporterStarted.get() == false) {
                KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(verifiableProps);
                if (metricsConfig.reporters.size() > 0) {
                    Utils.foreach(metricsConfig.reporters, new Callable1<String>() {
                        @Override
                        public void apply(String reporterType) {
                            KafkaMetricsReporter reporter = Utils.<KafkaMetricsReporter>createObject(reporterType);
                            reporter.init(verifiableProps);
                            if (reporter instanceof KafkaMetricsReporterMBean)
                                Utils.registerMBean(reporter, ((KafkaMetricsReporterMBean) reporter).getMBeanName());

                        }
                    });
                    ReporterStarted.set(true);
                }
            }
        }
    }
}
