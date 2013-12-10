package kafka.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedItem<T> implements Delayed {
    public final T item;
    public final long delay;
    public final TimeUnit unit;

    public DelayedItem(T item, long delay, TimeUnit unit) {
        this.item = item;
        this.delay = delay;
        this.unit = unit;

        createdMs = SystemTime.instance.milliseconds();
        long given = unit.toMillis(delay);
        delayMs = (given < 0 || (createdMs + given) < 0) ? (Long.MAX_VALUE - createdMs) : given;
    }

    public DelayedItem(T item, long delayMs) {
        this(item, delayMs, TimeUnit.MILLISECONDS);
    }

    public long createdMs;
    public long delayMs;

    /**
     * The remaining delay time
     */
    @Override
    public long getDelay(TimeUnit unit) {
        long elapsedMs = (SystemTime.instance.milliseconds() - createdMs);
        return unit.convert(Math.max(delayMs - elapsedMs, 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed d) {
        DelayedItem delayed = (DelayedItem) d;
        long myEnd = createdMs + delayMs;
        long yourEnd = delayed.createdMs + delayed.delayMs;

        if (myEnd < yourEnd) return -1;
        else if (myEnd > yourEnd) return 1;
        else return 0;
    }
}
