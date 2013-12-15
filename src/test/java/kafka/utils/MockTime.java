package kafka.utils;

import java.util.concurrent.TimeUnit;

/**
 * A class used for unit testing things which depend on the Time interface.
 * <p/>
 * This class never manually advances the clock, it only does so when you call
 * sleep(ms)
 * <p/>
 * It also comes with an associated scheduler instance for managing background tasks in
 * a deterministic way.
 */
public class MockTime implements Time {
    volatile private long currentMs;

    public MockTime(long currentMs) {
        this.currentMs = currentMs;
    }


    public MockScheduler scheduler = new MockScheduler(this);

    public MockTime() {
        this(System.currentTimeMillis());
    }

    @Override
    public long milliseconds() {
        return currentMs;
    }

    @Override
    public long nanoseconds() {
        return TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void sleep(long ms) {
        this.currentMs += ms;
        scheduler.tick();
    }

    @Override
    public String toString() {
        return String.format("MockTime(%d)", milliseconds());
    }
}
