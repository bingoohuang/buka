package kafka.utils;

/**
 * A mockable interface for time functions
 */
public interface Time {
    long milliseconds();

    long nanoseconds();

    void sleep(long ms);
}
