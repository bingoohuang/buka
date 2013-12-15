package kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class Throttler {
    public double desiredRatePerSec;
    public long checkIntervalMs;
    public boolean throttleDown;
    public Time time;


    /**
     * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
     * (the units of the process don't matter, it could be bytes or a count of some other thing), and will sleep for
     * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
     *
     * @param desiredRatePerSec: The rate we want to hit in units/sec
     * @param checkIntervalMs:   The interval at which to check our rate
     * @param throttleDown:      Does throttling increase or decrease our rate?
     * @param time:              The time implementation to use
     */
    public Throttler(double desiredRatePerSec, long checkIntervalMs, boolean throttleDown, Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.time = time;

        periodStartNs = time.nanoseconds();
    }

    public Throttler(double desiredRatePerSec) {
        this(desiredRatePerSec, 100L, true, SystemTime.instance);
    }

    private Object lock = new Object();
    private long periodStartNs;
    private double observedSoFar;

    Logger logger = LoggerFactory.getLogger(Throttler.class);

    public void maybeThrottle(double observed) {
        synchronized (lock) {
            observedSoFar += observed;
            long now = time.nanoseconds();
            long elapsedNs = now - periodStartNs;
            // if we have completed an interval AND we have observed something, maybe
            // we should take a little nap
            if (elapsedNs > checkIntervalMs * Times.NsPerMs && observedSoFar > 0) {
                double rateInSecs = (observedSoFar * Times.NsPerSec) / elapsedNs;
                boolean needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec));
                if (needAdjustment) {
                    // solve for the amount of time to sleep to make us hit the desired rate
                    double desiredRateMs = desiredRatePerSec / (double) Times.MsPerSec;
                    long elapsedMs = elapsedNs / Times.NsPerMs;
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
                    if (sleepTime > 0) {
                        logger.trace("Natural rate is {} per second but desired rate is {}, sleeping for {} ms to compensate.", rateInSecs, desiredRatePerSec, sleepTime);
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = now;
                observedSoFar = 0;
            }
        }
    }
}
