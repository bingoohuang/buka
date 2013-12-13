package kafka.log;

import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

import java.util.concurrent.TimeUnit;

public class LogFlushStats extends KafkaMetricsGroup {
    public static final LogFlushStats instance = new LogFlushStats();

    public KafkaTimer logFlushTimer = new KafkaTimer(newTimer("LogFlushRateAndTimeMs",
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
}
