package kafka.log;

import com.google.common.collect.Lists;
import kafka.common.OptimisticLockFailureException;
import kafka.message.*;
import kafka.utils.Function1;
import kafka.utils.Throttler;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;


public class Cleaner {
    public int id;
    public OffsetMap offsetMap;
    public int ioBufferSize;
    public int maxIoBufferSize;
    public double dupBufferLoadFactor;
    public Throttler throttler;
    public Time time;

    /**
     * This class holds the actual logic for cleaning a log
     *
     * @param id           An identifier used for logging
     * @param offsetMap    The map used for deduplication
     * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
     * @param throttler    The throttler instance to use for limiting I/O rate.
     * @param time         The time instance
     */
    public Cleaner(int id,
                   OffsetMap offsetMap,
                   int ioBufferSize,
                   int maxIoBufferSize,
                   double dupBufferLoadFactor,
                   Throttler throttler,
                   Time time) {
        this.id = id;
        this.offsetMap = offsetMap;
        this.ioBufferSize = ioBufferSize;
        this.maxIoBufferSize = maxIoBufferSize;
        this.dupBufferLoadFactor = dupBufferLoadFactor;
        this.throttler = throttler;
        this.time = time;

        logger = LoggerFactory.getLogger(Cleaner.class + "-" + id);
        stats = new CleanerStats(time);
        readBuffer = ByteBuffer.allocate(ioBufferSize);
        writeBuffer = ByteBuffer.allocate(ioBufferSize);
    }

    Logger logger;

    /* stats on this cleaning */
    public CleanerStats stats;

    /* buffer used for read i/o */
    private ByteBuffer readBuffer;

    /* buffer used for write i/o */
    private ByteBuffer writeBuffer;

    /**
     * Clean the given log
     *
     * @param cleanable The log to be cleaned
     * @return The first offset not cleaned
     */
    public long clean(LogToClean cleanable) throws InterruptedException {
        stats.clear();
        logger.info("Beginning cleaning of log {}.", cleanable.log.name());
        Log log = cleanable.log;
        int truncateCount = log.numberOfTruncates();

        // build the offset map
        logger.info("Building offset map for {}...", cleanable.log.name());
        long upperBoundOffset = log.activeSegment().baseOffset;
        long endOffset = buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap) + 1;
        stats.indexDone();

        // figure out the timestamp below which it is safe to remove delete tombstones
        // this position is defined to be a configurable time beneath the last modified time of the last clean segment
        LogSegment seg =
                Utils.lastOption(log.logSegments(0, cleanable.firstDirtyOffset));
        long deleteHorizonMs = seg == null ? 0L : (seg.lastModified() - log.config.deleteRetentionMs);

        // group the segments and clean the groups
        logger.info("Cleaning log {} (discarding tombstones prior to {})...", log.name(), new Date(deleteHorizonMs));
        for (List<LogSegment> group : groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize))
            cleanSegments(log, group, offsetMap, truncateCount, deleteHorizonMs);

        stats.allDone();
        return endOffset;
    }

    /**
     * Clean a group of segments into a single replacement segment
     *
     * @param log                   The log being cleaned
     * @param segments              The group of segments being cleaned
     * @param map                   The offset map to use for cleaning segments
     * @param expectedTruncateCount A count used to check if the log is being truncated and rewritten under our feet
     * @param deleteHorizonMs       The time to retain delete tombstones
     */
    void cleanSegments(Log log,
                       List<LogSegment> segments,
                       OffsetMap map,
                       int expectedTruncateCount,
                       long deleteHorizonMs) throws InterruptedException {
        // create a new segment with the suffix .cleaned appended to both the log and index name
        File logFile = new File(Utils.head(segments).log.file.getPath() + Logs.CleanedFileSuffix);
        logFile.delete();
        File indexFile = new File(Utils.head(segments).index.file.getPath() + Logs.CleanedFileSuffix);
        indexFile.delete();
        FileMessageSet messages = new FileMessageSet(logFile);
        OffsetIndex index = new OffsetIndex(indexFile, Utils.head(segments).baseOffset, Utils.head(segments).index.maxIndexSize);
        LogSegment cleaned = new LogSegment(messages, index, Utils.head(segments).baseOffset, Utils.head(segments).indexIntervalBytes, time);

        // clean segments into the new destination segment
        for (LogSegment old : segments) {
            boolean retainDeletes = old.lastModified() > deleteHorizonMs;
            logger.info("Cleaning segment {} in log {} (last modified {}) into {}, {} deletes.",
                    old.baseOffset, log.name(), new Date(old.lastModified()), cleaned.baseOffset, (retainDeletes ? "retaining" : "discarding"));
            cleanInto(old, cleaned, map, retainDeletes);
        }

        // trim excess index
        index.trimToValidSize();

        // flush new segment to disk before swap
        cleaned.flush();

        // update the modification date to retain the last modified date of the original files
        long modified = Utils.last(segments).lastModified();
        cleaned.lastModified(modified);

        // swap in new segment
        logger.info("Swapping in cleaned segment {} for segment(s) {} in log {}.", cleaned.baseOffset, Utils.mapList(segments, new Function1<LogSegment, Long>() {
            @Override
            public Long apply(LogSegment _) {
                return _.baseOffset;
            }
        }), log.name());
        try {
            log.replaceSegments(cleaned, segments, expectedTruncateCount);
        } catch (OptimisticLockFailureException e) {
            cleaned.delete();
            throw e;
        }
    }

    /**
     * Clean the given source log segment into the destination segment using the key=>offset mapping
     * provided
     *
     * @param source        The dirty log segment
     * @param dest          The cleaned log segment
     * @param map           The key=>offset mapping
     * @param retainDeletes Should delete tombstones be retained while cleaning this segment
     *                      <p/>
     *                      TODO: Implement proper compression support
     */
    private void cleanInto(LogSegment source, LogSegment dest, OffsetMap map, boolean retainDeletes) throws InterruptedException {
        int position = 0;
        while (position < source.log.sizeInBytes()) {
            checkDone();
            // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
            readBuffer.clear();
            writeBuffer.clear();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(source.log.readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes());
            // check each message to see if it is to be retained
            int messagesRead = 0;
            for (MessageAndOffset entry : messages) {
                messagesRead += 1;
                int size = MessageSets.entrySize(entry.message);
                position += size;
                stats.readMessage(size);
                ByteBuffer key = entry.message.key();
                checkState(key != null, String.format("Found null key in log segment %s which is marked as dedupe.", source.log.file.getAbsolutePath()));
                long foundOffset = map.get(key);
        /* two cases in which we can get rid of a message:
         *   1) if there exists a message with the same key but higher offset
         *   2) if the message is a delete "tombstone" marker and enough time has passed
         */
                boolean redundant = foundOffset >= 0 && entry.offset < foundOffset;
                boolean obsoleteDelete = !retainDeletes && entry.message.isNull();
                if (!redundant && !obsoleteDelete) {
                    ByteBufferMessageSets.writeMessage(writeBuffer, entry.message, entry.offset);
                    stats.recopyMessage(size);
                }
            }
            // if any messages are to be retained, write them out
            if (writeBuffer.position() > 0) {
                writeBuffer.flip();
                ByteBufferMessageSet retained = new ByteBufferMessageSet(writeBuffer);
                dest.append(Utils.head(retained).offset, retained);
                throttler.maybeThrottle(writeBuffer.limit());
            }

            // if we read bytes but didn't get even one complete message, our I/O buffer is too small, grow it and try again
            if (readBuffer.limit() > 0 && messagesRead == 0)
                growBuffers();
        }
        restoreBuffers();
    }

    /**
     * Double the I/O buffer capacity
     */
    public void growBuffers() {
        if (readBuffer.capacity() >= maxIoBufferSize || writeBuffer.capacity() >= maxIoBufferSize)
            throw new IllegalStateException(String.format("This log contains a message larger than maximum allowable size of %s.", maxIoBufferSize));
        int newSize = Math.min(this.readBuffer.capacity() * 2, maxIoBufferSize);
        logger.info("Growing cleaner I/O buffers from " + readBuffer.capacity() + "bytes to " + newSize + " bytes.");
        this.readBuffer = ByteBuffer.allocate(newSize);
        this.writeBuffer = ByteBuffer.allocate(newSize);
    }

    /**
     * Restore the I/O buffer capacity to its original size
     */
    public void restoreBuffers() {
        if (this.readBuffer.capacity() > this.ioBufferSize)
            this.readBuffer = ByteBuffer.allocate(this.ioBufferSize);
        if (this.writeBuffer.capacity() > this.ioBufferSize)
            this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize);
    }

    /**
     * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
     * We collect a group of such segments together into a single
     * destination segment. This prevents segment sizes from shrinking too much.
     *
     * @param segments     The log segments to group
     * @param maxSize      the maximum size in bytes for the total of all log data in a group
     * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
     * @return A list of grouped segments
     */
    List<List<LogSegment>> groupSegmentsBySize(Iterable<LogSegment> segments, int maxSize, int maxIndexSize) {
        List<List<LogSegment>> grouped = Lists.newArrayList();
        List<LogSegment> segs = Lists.newArrayList(segments);
        while (!segs.isEmpty()) {
            List<LogSegment> group = Lists.newArrayList(Utils.head(segs));
            long logSize = Utils.head(segs).size();
            int indexSize = Utils.head(segs).index.sizeInBytes();
            segs = Utils.tail(segs);
            while (!segs.isEmpty() &&
                    logSize + Utils.head(segs).size() < maxSize &&
                    indexSize + Utils.head(segs).index.sizeInBytes() < maxIndexSize) {
                group.add(Utils.head(segs));
                logSize += Utils.head(segs).size();
                indexSize += Utils.head(segs).index.sizeInBytes();
                segs = Utils.tail(segs);
            }
            grouped.add(group);
        }

        return grouped;
    }

    /**
     * Build a map of key_hash => offset for the keys in the dirty portion of the log to use in cleaning.
     *
     * @param log   The log to use
     * @param start The offset at which dirty messages begin
     * @param end   The ending offset for the map that is being built
     * @param map   The map in which to store the mappings
     * @return The final offset the map covers
     */
    long buildOffsetMap(Log log, long start, long end, OffsetMap map) throws InterruptedException {
        map.clear();
        Collection<LogSegment> dirty = log.logSegments(start, end);
        logger.info("Building offset map for log {} for {} segments in offset range [{}, {}).", log.name(), dirty.size(), start, end);

        // Add all the dirty segments. We must take at least map.slots * load_factor,
        // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
        long offset = Utils.head(dirty).baseOffset;
        checkState(offset == start, String.format("Last clean offset is %d but segment base offset is %d for log %s.", start, offset, log.name()));
        long minStopOffset = (long) (start + map.slots() * this.dupBufferLoadFactor);
        for (LogSegment segment : dirty) {
            checkDone();
            if (segment.baseOffset <= minStopOffset || map.utilization() < this.dupBufferLoadFactor)
                offset = buildOffsetMap(segment, map);
        }
        logger.info("Offset map for log {} complete.", log.name());
        return offset;
    }

    /**
     * Add the messages in the given segment to the offset map
     *
     * @param segment The segment to index
     * @param map     The map in which to store the key=>offset mapping
     * @return The final offset covered by the map
     */
    private long buildOffsetMap(LogSegment segment, OffsetMap map) throws InterruptedException {
        int position = 0;
        long offset = segment.baseOffset;
        while (position < segment.log.sizeInBytes()) {
            checkDone();
            readBuffer.clear();
            ByteBufferMessageSet messages = new ByteBufferMessageSet(segment.log.readInto(readBuffer, position));
            throttler.maybeThrottle(messages.sizeInBytes());
            int startPosition = position;
            for (MessageAndOffset entry : messages) {
                Message message = entry.message;
                checkState(message.hasKey());
                int size = MessageSets.entrySize(message);
                position += size;
                map.put(message.key(), entry.offset);
                offset = entry.offset;
                stats.indexMessage(size);
            }
            // if we didn't read even one complete message, our read buffer may be too small
            if (position == startPosition)
                growBuffers();
        }
        restoreBuffers();
        return offset;
    }

    /**
     * If we aren't running any more throw an AllDoneException
     */
    private void checkDone() throws InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
    }
}
