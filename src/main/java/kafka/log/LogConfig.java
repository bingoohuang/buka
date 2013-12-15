package kafka.log;

import kafka.cluster.LogConfigs;

import java.util.Properties;

public class LogConfig implements Cloneable {
    public int segmentSize = 1024*1024;
    public long segmentMs = Long.MAX_VALUE;
    public long flushInterval = Long.MAX_VALUE;
    public long flushMs = Long.MAX_VALUE;
    public long retentionSize = Long.MAX_VALUE;
    public long retentionMs = Long.MAX_VALUE;
    public int maxMessageSize = Integer.MAX_VALUE;
    public int maxIndexSize = 1024*1024;
    public int indexInterval = 4096;
    public long fileDeleteDelayMs = 60*1000;
    public long deleteRetentionMs = 24 * 60 * 60 * 1000L;
    public double minCleanableRatio = 0.5;
    public boolean dedupe = false;

    /**
     * Configuration settings for a log
     * @param segmentSize The soft maximum for the size of a segment file in the log
     * @param segmentMs The soft maximum on the amount of time before a new log segment is rolled
     * @param flushInterval The number of messages that can be written to the log before a flush is forced
     * @param flushMs The amount of time the log can have dirty data before a flush is forced
     * @param retentionSize The approximate total number of bytes this log can use
     * @param retentionMs The age approximate maximum age of the last segment that is retained
     * @param maxIndexSize The maximum size of an index file
     * @param indexInterval The approximate number of bytes between index entries
     * @param fileDeleteDelayMs The time to wait before deleting a file from the filesystem
     * @param deleteRetentionMs The time to retain delete markers in the log. Only applicable for logs that are being compacted.
     * @param minCleanableRatio The ratio of bytes that are available for cleaning to the bytes already cleaned
     * @param dedupe Should old segments in this log be deleted or deduplicated?
     */
    public LogConfig(int segmentSize,
                     long segmentMs,
                     long flushInterval,
                     long flushMs,
                     long retentionSize,
                     long retentionMs,
                     int maxMessageSize,
                     int maxIndexSize,
                     int indexInterval,
                     long fileDeleteDelayMs,
                     long deleteRetentionMs,
                     double minCleanableRatio,
                     boolean dedupe) {
        this.segmentSize = segmentSize;
        this.segmentMs = segmentMs;
        this.flushInterval = flushInterval;
        this.flushMs = flushMs;
        this.retentionSize = retentionSize;
        this.retentionMs = retentionMs;
        this.maxMessageSize = maxMessageSize;
        this.maxIndexSize = maxIndexSize;
        this.indexInterval = indexInterval;
        this.fileDeleteDelayMs = fileDeleteDelayMs;
        this.deleteRetentionMs = deleteRetentionMs;
        this.minCleanableRatio = minCleanableRatio;
        this.dedupe = dedupe;
    }

    public LogConfig() {

    }

    public Properties toProps()  {
        Properties props = new Properties();
        props.put(LogConfigs.SegmentBytesProp, segmentSize + "");
        props.put(LogConfigs.SegmentMsProp, segmentMs + "");
        props.put(LogConfigs.SegmentIndexBytesProp, maxIndexSize + "");
        props.put(LogConfigs.FlushMessagesProp, flushInterval + "");
        props.put(LogConfigs.FlushMsProp, flushMs + "");
        props.put(LogConfigs.RetentionBytesProp, retentionSize + "");
        props.put(LogConfigs.RententionMsProp, retentionMs + "");
        props.put(LogConfigs.MaxMessageBytesProp, maxMessageSize + "");
        props.put(LogConfigs.IndexIntervalBytesProp, indexInterval + "");
        props.put(LogConfigs.DeleteRetentionMsProp, deleteRetentionMs + "");
        props.put(LogConfigs.FileDeleteDelayMsProp, fileDeleteDelayMs + "");
        props.put(LogConfigs.MinCleanableDirtyRatioProp, minCleanableRatio + "");
        props.put(LogConfigs.CleanupPolicyProp, (dedupe) ? "dedupe" : "delete");
        return props;
    }

    @Override
    protected LogConfig clone()  {
        try {
            return (LogConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }
}
