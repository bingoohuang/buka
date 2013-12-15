package kafka.log;

import kafka.common.TopicAndPartition;
import kafka.utils.Function2;
import kafka.utils.Utils;

import java.util.Collection;

/**
 * Helper class for a log, its topic/partition, and the last clean position
 */
public class LogToClean implements Comparable<LogToClean> {
    public TopicAndPartition topicPartition;
    public Log log;
    public long firstDirtyOffset;

    public LogToClean(TopicAndPartition topicPartition, Log log, long firstDirtyOffset) {
        this.topicPartition = topicPartition;
        this.log = log;
        this.firstDirtyOffset = firstDirtyOffset;
        cleanBytes = Utils.foldLeft(log.logSegments(-1, firstDirtyOffset - 1), 0L, new Function2<Long, LogSegment, Long>() {
            @Override
            public Long apply(Long arg1, LogSegment _) {
                return arg1 + _.size();
            }
        });

        Collection<LogSegment> values = log.logSegments(firstDirtyOffset, Math.max(firstDirtyOffset, log.activeSegment().baseOffset));
        dirtyBytes = Utils.foldLeft(values, 0L, new Function2<Long, LogSegment, Long>() {
            @Override
            public Long apply(Long arg1, LogSegment _) {
                return arg1 + _.size();
            }
        });

        cleanableRatio = dirtyBytes / (double) totalBytes();
    }

    public long cleanBytes;
    public long dirtyBytes;
    public double cleanableRatio;

    public long totalBytes() {
        return cleanBytes + dirtyBytes;
    }

    @Override
    public int compareTo(LogToClean that) {
        return (int) Math.signum(this.cleanableRatio - that.cleanableRatio);
    }
}
