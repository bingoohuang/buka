package kafka.metrics;

import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import kafka.utils.Function0;

public class KafkaTimer {
    public Timer metric;

    /**
     * A wrapper around metrics timer object that provides a convenient mechanism
     * to time code blocks. This pattern was borrowed from the metrics-scala_2.9.1
     * package.
     * @param metric The underlying timer object.
     */
    public KafkaTimer(Timer metric) {
        this.metric = metric;
    }

    public <A> A time(Function0<A> f) {
        TimerContext ctx = metric.time();
        try {
            return f.apply();
        }
        finally {
            ctx.stop();
        }
    }
}
