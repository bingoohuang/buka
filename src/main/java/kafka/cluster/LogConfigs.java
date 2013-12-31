package kafka.cluster;

import com.google.common.collect.Sets;
import kafka.log.LogConfig;

import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class LogConfigs {
    public static final String SegmentBytesProp = "segment.bytes";
    public static final String SegmentMsProp = "segment.ms";
    public static final String SegmentIndexBytesProp = "segment.index.bytes";
    public static final String FlushMessagesProp = "flush.messages";
    public static final String FlushMsProp = "flush.ms";
    public static final String RetentionBytesProp = "retention.bytes";
    public static final String RententionMsProp = "retention.ms";
    public static final String MaxMessageBytesProp = "max.message.bytes";
    public static final String IndexIntervalBytesProp = "index.interval.bytes";
    public static final String DeleteRetentionMsProp = "delete.retention.ms";
    public static final String FileDeleteDelayMsProp = "file.delete.delay.ms";
    public static final String MinCleanableDirtyRatioProp = "min.cleanable.dirty.ratio";
    public static final String CleanupPolicyProp = "cleanup.policy";

    public static Set<String> ConfigNames = Sets.newHashSet(SegmentBytesProp,
            SegmentMsProp,
            SegmentIndexBytesProp,
            FlushMessagesProp,
            FlushMsProp,
            RetentionBytesProp,
            RententionMsProp,
            MaxMessageBytesProp,
            IndexIntervalBytesProp,
            FileDeleteDelayMsProp,
            DeleteRetentionMsProp,
            MinCleanableDirtyRatioProp,
            CleanupPolicyProp);


    /**
     * Parse the given properties instance into a LogConfig object
     */
    public static LogConfig fromProps(Properties props) {
        return new LogConfig(/*segmentSize =*/ Integer.parseInt(props.getProperty(SegmentBytesProp)),
                /*segmentMs =*/ Long.parseLong(props.getProperty(SegmentMsProp)),
                /*flushInterval =*/ Long.parseLong(props.getProperty(FlushMessagesProp)),
                /*flushMs =*/ Long.parseLong(props.getProperty(FlushMsProp)),
                /*retentionSize =*/ Long.parseLong(props.getProperty(RetentionBytesProp)),
                /*retentionMs =*/ Long.parseLong(props.getProperty(RententionMsProp)),
                /*maxMessageSize = */ Integer.parseInt(props.getProperty(MaxMessageBytesProp)),
                /*maxIndexSize =*/ Integer.parseInt(props.getProperty(SegmentIndexBytesProp)),
                /*indexInterval =*/ Integer.parseInt(props.getProperty(IndexIntervalBytesProp)),
                /*fileDeleteDelayMs =*/ Integer.parseInt(props.getProperty(FileDeleteDelayMsProp)),
                /*deleteRetentionMs = */Long.parseLong(props.getProperty(DeleteRetentionMsProp)),
                /*minCleanableRatio =*/ Double.parseDouble(props.getProperty(MinCleanableDirtyRatioProp)),
               /* dedupe =*/ props.getProperty(CleanupPolicyProp).trim().toLowerCase() == "dedupe");
    }

    /**
     * Create a log config instance using the given properties and defaults
     */
    public static LogConfig fromProps(Properties defaults, Properties overrides) {
        Properties props = new Properties(defaults);
        props.putAll(overrides);
        return fromProps(props);
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        for (Object name : props.keySet())
            checkState(ConfigNames.contains(name), String.format("Unknown configuration \"%s\".", name));
    }

    /**
     * Check that the given properties contain only valid log config names, and that all values can be parsed.
     */
    public static void validate(Properties props) {
        validateNames(props);
        fromProps(new LogConfig().toProps(), props); // check that we can parse the values
    }
}
