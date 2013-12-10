package kafka.utils;


/**
 * The normal system implementation of time functions
 */
public class SystemTime implements Time {
    public static final SystemTime instance = new SystemTime();

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {

        }
    }
}
