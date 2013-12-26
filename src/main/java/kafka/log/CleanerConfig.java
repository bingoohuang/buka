package kafka.log;

public class CleanerConfig {
    public int numThreads = 1;
    public long dedupeBufferSize = 4*1024*1024L;
    public double dedupeBufferLoadFactor = 0.9d;
    public int ioBufferSize = 1024*1024;
    public int maxMessageSize = 32*1024*1024;
    public double maxIoBytesPerSecond = Double.MAX_VALUE;
    public long backOffMs = 60 * 1000;
    public boolean enableCleaner = true;
    public String hashAlgorithm = "MD5";


    /**
     * Configuration parameters for the log cleaner
     *
     * @param numThreads The number of cleaner threads to run
     * @param dedupeBufferSize The total memory used for log deduplication
     * @param dedupeBufferLoadFactor The maximum percent full for the deduplication buffer
     * @param maxMessageSize The maximum size of a message that can appear in the log
     * @param maxIoBytesPerSecond The maximum read and write I/O that all cleaner threads are allowed to do
     * @param backOffMs The amount of time to wait before rechecking if no logs are eligible for cleaning
     * @param enableCleaner Allows completely disabling the log cleaner
     * @param hashAlgorithm The hash algorithm to use in key comparison.
     */
    public CleanerConfig(int numThreads,
                         long dedupeBufferSize,
                         double dedupeBufferLoadFactor,
                         int ioBufferSize,
                         int maxMessageSize,
                         double maxIoBytesPerSecond,
                         long backOffMs,
                         boolean enableCleaner,
                         String hashAlgorithm) {
        this.numThreads = numThreads;
        this.dedupeBufferSize = dedupeBufferSize;
        this.dedupeBufferLoadFactor = dedupeBufferLoadFactor;
        this.ioBufferSize = ioBufferSize;
        this.maxMessageSize = maxMessageSize;
        this.maxIoBytesPerSecond = maxIoBytesPerSecond;
        this.backOffMs = backOffMs;
        this.enableCleaner = enableCleaner;
        this.hashAlgorithm = hashAlgorithm;
    }

    public CleanerConfig() {
    }
}
