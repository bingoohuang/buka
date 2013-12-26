package kafka.log;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.yammer.metrics.core.Gauge;
import kafka.common.*;
import kafka.message.*;
import kafka.metrics.KafkaMetricsGroup;
import kafka.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


@ThreadSafe
public class Log extends KafkaMetricsGroup implements Closeable {
    public File dir;
    public volatile LogConfig config;
    public volatile long recoveryPoint = 0L;
    public Scheduler scheduler;
    public Time time = SystemTime.instance;

    /**
     * An append-only log for storing messages.
     * <p/>
     * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
     * <p/>
     * New log segments are created according to a configurable policy that controls the size in bytes or time interval
     * for a given segment.
     *
     * @param dir           The directory in which log segments are created.
     * @param config        The log configuration settings
     * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
     * @param scheduler     The thread pool scheduler used for background actions
     * @param time          The time instance used for checking the clock
     */
    public Log(File dir, LogConfig config, long recoveryPoint, Scheduler scheduler, Time time) {
        this.dir = dir;
        this.config = config;
        this.recoveryPoint = recoveryPoint;
        this.scheduler = scheduler;
        this.time = time;

        lastflushedTime = new AtomicLong(time.milliseconds());
        loadSegments();
        nextOffset = new AtomicLong(activeSegment().nextOffset());


        logger.info("Completed load of log {} with log end offset {}", name(), logEndOffset());

        newGauge(name() + "-" + "NumLogSegments",
                new Gauge<Integer>() {

                    @Override
                    public Integer value() {
                        return numberOfSegments();
                    }
                });

        newGauge(name() + "-" + "LogEndOffset",
                new Gauge<Long>() {

                    @Override
                    public Long value() {
                        return logEndOffset();
                    }
                });
    }

    Logger logger = LoggerFactory.getLogger(Log.class);


    /* A lock that guards all modifications to the log */
    private Object lock = new Object();

    /* last time it was flushed */
    private AtomicLong lastflushedTime;

    /* the actual segments of the log */
    private ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<Long, LogSegment>();


    /* The number of times the log has been truncated */
    private AtomicInteger truncates = new AtomicInteger(0);

    /* Calculate the offset of the next message */
    private AtomicLong nextOffset;


    /**
     * The name of this log
     */
    public String name() {
        return dir.getName();
    }

    /* Load the log segments from the log files on disk */
    private void loadSegments() {
        // create the log directory if it doesn't exist
        dir.mkdirs();

        // first do a pass through the files in the log directory and remove any temporary files
        // and complete any interrupted swap operations
        for (File file : dir.listFiles()) {
            if (!file.isFile()) continue;

            if (!file.canRead()) throw new KafkaException("Could not read file " + file);

            String filename = file.getName();
            if (filename.endsWith(Logs.DeletedFileSuffix) || filename.endsWith(Logs.CleanedFileSuffix)) {
                // if the file ends in .deleted or .cleaned, delete it
                file.delete();
            } else if (filename.endsWith(Logs.SwapFileSuffix)) {
                // we crashed in the middle of a swap operation, to recover:
                // if a log, swap it in and delete the .index file
                // if an index just delete it, it will be rebuilt
                File baseName = new File(Utils.replaceSuffix(file.getPath(), Logs.SwapFileSuffix, ""));
                if (baseName.getPath().endsWith(Logs.IndexFileSuffix)) {
                    file.delete();
                } else if (baseName.getPath().endsWith(Logs.LogFileSuffix)) {
                    // delete the index
                    File index = new File(Utils.replaceSuffix(baseName.getPath(), Logs.LogFileSuffix, Logs.IndexFileSuffix));
                    index.delete();
                    // complete the swap operation
                    boolean renamed = file.renameTo(baseName);
                    if (renamed)
                        logger.info("Found log file {} from interrupted swap operation, repairing.", file.getPath());
                    else
                        throw new KafkaException("Failed to rename file %s.", file.getPath());
                }
            }
        }

        // now do a second pass and load all the .log and .index files
        for (File file : dir.listFiles()) {
            if (!file.isFile()) continue;

            String filename = file.getName();
            if (filename.endsWith(Logs.IndexFileSuffix)) {
                // if it is an index file, make sure it has a corresponding .log file
                File logFile = new File(file.getAbsolutePath().replace(Logs.IndexFileSuffix, Logs.LogFileSuffix));
                if (!logFile.exists()) {
                    logger.warn("Found an orphaned index file, {}, with no corresponding log file.", file.getAbsolutePath());
                    file.delete();
                }
            } else if (filename.endsWith(Logs.LogFileSuffix)) {
                // if its a log file, load the corresponding log segment
                long start = Long.parseLong(filename.substring(0, filename.length() - Logs.LogFileSuffix.length()));
                boolean hasIndex = Logs.indexFilename(dir, start).exists();
                LogSegment segment = new LogSegment(dir,
                        /*startOffset = */start,
                       /* indexIntervalBytes =*/ config.indexInterval,
                       /* maxIndexSize =*/ config.maxIndexSize,
                        time);
                if (!hasIndex) {
                    logger.error("Could not find index file corresponding to log file {}, rebuilding index...", segment.log.file.getAbsolutePath());
                    segment.recover(config.maxMessageSize);
                }
                segments.put(start, segment);
            }
        }

        if (logSegments().size() == 0) {
            // no existing segments, create a new mutable segment beginning at offset 0
            segments.put(0L, new LogSegment(dir,
                    /*startOffset =*/ 0L,
                    /*indexIntervalBytes = */config.indexInterval,
                   /* maxIndexSize = */config.maxIndexSize,
                    time));
        } else {
            recoverLog();
            // reset the index size of the currently active log segment to allow more entries
            activeSegment().index.resize(config.maxIndexSize);
        }

        // sanity check the index file of every segment to ensure we don't proceed with a corrupt segment
        for (LogSegment s : logSegments())
            s.index.sanityCheck();
    }

    private void recoverLog() {
        // if we have the clean shutdown marker, skip recovery
        if (hasCleanShutdownFile()) {
            this.recoveryPoint = activeSegment().nextOffset();
            return;
        }

        // okay we need to actually recovery this log
        Iterator<LogSegment> unflushed = logSegments(this.recoveryPoint, Long.MAX_VALUE).iterator();
        while (unflushed.hasNext()) {
            LogSegment curr = unflushed.next();
            logger.info("Recovering unflushed segment {} in log {}.", curr.baseOffset, name());
            int truncatedBytes = 0;
            try {
                truncatedBytes = curr.recover(config.maxMessageSize);
            } catch (InvalidOffsetException e) {
                long startOffset = curr.baseOffset;
                logger.warn("Found invalid offset during recovery for log " + dir.getName() + ". Deleting the corrupt segment and " +
                        "creating an empty one with starting offset " + startOffset);
                curr.truncateTo(startOffset);
            }
            if (truncatedBytes > 0) {
                // we had an invalid message, delete all remaining log
                logger.warn("Corruption found in segment {} of log {}, truncating to offset {}.", curr.baseOffset, name(), curr.nextOffset());
                Utils.foreach(unflushed, new Callable1<LogSegment>() {
                    @Override
                    public void apply(LogSegment arg) {
                        deleteSegment(arg);
                    }
                });
            }
        }
    }


    /**
     * Check if we have the "clean shutdown" file
     */
    private boolean hasCleanShutdownFile() {
        return new File(dir.getParentFile(), Logs.CleanShutdownFile).exists();
    }

    /**
     * The number of segments in the log.
     * Take care! this is an O(n) operation.
     */
    public int numberOfSegments() {
        return segments.size();
    }

    /**
     * The number of truncates that have occurred since the log was opened.
     */
    public int numberOfTruncates() {
        return truncates.get();
    }

    /**
     * Close this log
     */
    public void close() {
        logger.debug("Closing log {}", name());
        synchronized (lock) {
            for (LogSegment seg : logSegments())
                seg.close();
        }
    }

    public LogAppendInfo append(ByteBufferMessageSet messages) throws KafkaStorageException {
        return append(messages, true);
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     * <p/>
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param messages      The message set to append
     * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
     * @return Information about the appended messages including the first and last offset.
     * @throws KafkaStorageException If the append fails due to an I/O error.
     */
    public LogAppendInfo append(ByteBufferMessageSet messages, Boolean assignOffsets /* = true*/) throws KafkaStorageException {
        LogAppendInfo appendInfo = analyzeAndValidateMessageSet(messages);

        // if we have any valid messages, append them to the log
        if (appendInfo.shallowCount == 0)
            return appendInfo;

        // trim any invalid bytes or partial messages before appending it to the on-disk log
        ByteBufferMessageSet validMessages = trimInvalidBytes(messages);

        try {
            // they are valid, insert them in the log
            synchronized (lock) {
                appendInfo.firstOffset = nextOffset.get();

                // maybe roll the log if this segment is full
                LogSegment segment = maybeRoll();

                if (assignOffsets) {
                    // assign offsets to the messageset
                    AtomicLong offset = new AtomicLong(nextOffset.get());
                    try {
                        validMessages = validMessages.assignOffsets(offset, appendInfo.codec);
                    } catch (Throwable e) {
                        throw new KafkaException(e, "Error in validating messages while appending to log '%s'", name());
                    }
                    appendInfo.lastOffset = offset.get() - 1;
                } else {
                    // we are taking the offsets we are given
                    if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffset.get())
                        throw new IllegalArgumentException("Out of order offsets found in " + messages);
                }

                // Check if the message sizes are valid. This check is done after assigning offsets to ensure the comparison
                // happens with the new message size (after re-compression, if any)
                Iterator<MessageAndOffset> iterator = validMessages.shallowIterator();
                while (iterator.hasNext()) {
                    MessageAndOffset messageAndOffset = iterator.next();
                    if (MessageSets.entrySize(messageAndOffset.message) > config.maxMessageSize)
                        throw new MessageSizeTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d.",
                                MessageSets.entrySize(messageAndOffset.message), config.maxMessageSize);
                }

                // now append to the log
                segment.append(appendInfo.firstOffset, validMessages);

                // increment the log end offset
                nextOffset.set(appendInfo.lastOffset + 1);

                logger.trace("Appended message set to log {} with first offset: {}, next offset: {}, and messages: {}",
                        this.name(), appendInfo.firstOffset, nextOffset.get(), validMessages);

                if (unflushedMessages() >= config.flushInterval)
                    flush();

                return appendInfo;
            }
        } catch (Exception e) {
            throw new KafkaStorageException(e, "I/O exception in append to log '%s'", name());
        }
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * </ol>
     * <p/>
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private LogAppendInfo analyzeAndValidateMessageSet(ByteBufferMessageSet messages) {
        int messageCount = 0;
        long firstOffset = -1L, lastOffset = -1L;
        CompressionCodec codec = NoCompressionCodec.instance;
        boolean monotonic = true;

        Iterator<MessageAndOffset> messageAndOffsetIterator = messages.shallowIterator();
        while (messageAndOffsetIterator.hasNext()) {
            MessageAndOffset messageAndOffset = messageAndOffsetIterator.next();
            // update the first offset if on the first message
            if (firstOffset < 0)
                firstOffset = messageAndOffset.offset;
            // check that offsets are monotonically increasing
            if (lastOffset >= messageAndOffset.offset)
                monotonic = false;
            // update the last offset seen
            lastOffset = messageAndOffset.offset;

            // check the validity of the message by checking CRC
            Message m = messageAndOffset.message;
            m.ensureValid();
            messageCount += 1;

            CompressionCodec messageCodec = m.compressionCodec();
            if (messageCodec != NoCompressionCodec.instance)
                codec = messageCodec;
        }
        return new LogAppendInfo(firstOffset, lastOffset, codec, messageCount, monotonic);
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param messages The message set to trim
     * @return A trimmed message set. This may be the same as what was passed in or it may not.
     */
    private ByteBufferMessageSet trimInvalidBytes(ByteBufferMessageSet messages) {
        int messageSetValidBytes = messages.validBytes();
        if (messageSetValidBytes < 0)
            throw new InvalidMessageSizeException("Illegal length of message set " + messageSetValidBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests");
        if (messageSetValidBytes == messages.sizeInBytes()) {
            return messages;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = messages.buffer.duplicate();
            validByteBuffer.limit(messageSetValidBytes);
            return new ByteBufferMessageSet(validByteBuffer);
        }
    }

    public MessageSet read(long startOffset, Integer maxLength) {
        return read(startOffset, maxLength, null);
    }

    /**
     * Read messages from the log
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength   The maximum number of bytes to read
     * @param maxOffset   -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).
     * @return The messages read
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
     */

    public MessageSet read(long startOffset, int maxLength, Long maxOffset /*= None*/) {
        logger.trace("Reading {} bytes from offset {} in log {} of length {} bytes", maxLength, startOffset, name(), size());

        // check if the offset is valid and in range
        long next = nextOffset.get();
        if (startOffset == next)
            return MessageSets.Empty;

        Map.Entry<Long, LogSegment> entry = segments.floorEntry(startOffset);

        // attempt to read beyond the log end offset is an error
        if (startOffset > next || entry == null)
            throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.", startOffset, segments.firstKey(), next);

        // do the read on the segment with a base offset less than the target offset
        // but if that segment doesn't contain any messages with an offset greater than that
        // continue to read from successive segments until we get some messages or we reach the end of the log
        while (entry != null) {
            MessageSet messages = entry.getValue().read(startOffset, maxOffset, maxLength);
            if (messages == null)
                entry = segments.higherEntry(entry.getKey());
            else
                return messages;
        }

        // okay we are beyond the end of the last segment but less than the log end offset
        return MessageSets.Empty;
    }

    /**
     * Delete any log segments matching the given predicate function,
     * starting with the oldest segment and moving forward until a segment doesn't match.
     *
     * @param predicate A function that takes in a single log segment and returns true iff it is deletable
     * @return The number of segments deleted
     */
    public Integer deleteOldSegments(final Predicate<LogSegment> predicate) {
        // find any segments that match the user-supplied predicate UNLESS it is the final segment
        // and it is empty (since we would just end up re-creating it
        final LogSegment lastSegment = activeSegment();

        List<LogSegment> deletable = Utils.filter(logSegments(), new Predicate<LogSegment>() {
            @Override
            public boolean apply(LogSegment s) {
                return predicate.apply(s) && (s.baseOffset != lastSegment.baseOffset || s.size() > 0);
            }
        });

        int numToDelete = deletable.size();
        if (numToDelete > 0) {
            synchronized (lock) {
                // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
                if (segments.size() == numToDelete)
                    roll();
                // remove the segments for lookups
                Utils.foreach(deletable, new Callable1<LogSegment>() {
                    @Override
                    public void apply(LogSegment _) {
                        deleteSegment(_);
                    }
                });
            }
        }
        return numToDelete;
    }

    /**
     * The size of the log in bytes
     */
    public long size() {
        return Utils.foldLeft(logSegments(), 0L, new Function2<Long, LogSegment, Long>() {
            @Override
            public Long apply(Long arg1, LogSegment _) {
                return arg1 + _.size();
            }
        });
    }


    /**
     * The offset of the next message that will be appended to the log
     */
    public long logEndOffset() {
        return nextOffset.get();
    }

    /**
     * Roll the log over to a new empty log segment if necessary
     *
     * @return The currently active segment after (perhaps) rolling to a new segment
     */
    private LogSegment maybeRoll() {
        LogSegment segment = activeSegment();
        if (segment.size() > config.segmentSize ||
                segment.size() > 0 && time.milliseconds() - segment.created > config.segmentMs ||
                segment.index.isFull()) {
            logger.debug("Rolling new log segment in {} (log_size = {}/{}, index_size = {}/{}, age_ms = {}/{}).",
                    name(), segment.size(), config.segmentSize,
                    segment.index.entries(),
                    segment.index.maxEntries,
                    time.milliseconds() - segment.created,
                    config.segmentMs);
            return roll();
        } else {
            return segment;
        }
    }

    /**
     * Roll the log over to a new active segment starting with the current logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @return The newly rolled segment
     */
    public LogSegment roll() {
        long start = time.nanoseconds();
        synchronized (lock) {
            final Long newOffset = logEndOffset();
            File logFile = Logs.logFilename(dir, newOffset);
            File indexFile = Logs.indexFilename(dir, newOffset);
            for (File file : ImmutableList.of(logFile, indexFile)) {
                if (!file.exists()) continue;
                logger.warn("Newly rolled segment file " + file.getName() + " already exists; deleting it first");
                file.delete();
            }

            Map.Entry<Long, LogSegment> entry = segments.lastEntry();
            if (entry != null) entry.getValue().index.trimToValidSize();

            LogSegment segment = new LogSegment(dir,
                    /*startOffset = */newOffset,
                    config.indexInterval,
                    config.maxIndexSize,
                    time);
            LogSegment prev = addSegment(segment);
            if (prev != null)
                throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.", name(), newOffset);

            // schedule an asynchronous flush of the old segment
            scheduler.schedule("flush-log", new Runnable() {
                @Override
                public void run() {
                    flush(newOffset);
                }
            }, /*delay =*/ 0L, -1, TimeUnit.MILLISECONDS);

            logger.info("Rolled new log segment for '{}' in {} ms.", name(), (System.nanoTime() - start) / (1000.0 * 1000.0));

            return segment;
        }
    }


    /**
     * The number of messages appended to the log since the last flush
     */
    public long unflushedMessages() {
        return this.logEndOffset() - this.recoveryPoint;
    }

    /**
     * Flush all log segments
     */
    public void flush() {
        flush(this.logEndOffset());
    }

    /**
     * Flush log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    public void flush(Long offset) {
        if (offset <= this.recoveryPoint)
            return;

        logger.debug("Flushing log {} up to offset {}, last flushed: {} current time: {} unflushed = {}",
                name(), offset, lastFlushTime(), time.milliseconds(), unflushedMessages());

        for (LogSegment segment : logSegments(this.recoveryPoint, offset))
            segment.flush();
        synchronized (lock) {
            if (offset > this.recoveryPoint) {
                this.recoveryPoint = offset;
                lastflushedTime.set(time.milliseconds());
            }
        }
    }

    /**
     * Completely delete this log directory and all contents from the file system with no delay
     */
    public void delete() {
        Utils.foreach(logSegments(), new Callable1<LogSegment>() {
            @Override
            public void apply(LogSegment _) {
                _.delete();
            }
        });
        Utils.rm(dir);
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     */
    public void truncateTo(final long targetOffset) {
        logger.info("Truncating log {} to offset {}.", name(), targetOffset);
        if (targetOffset < 0)
            throw new IllegalArgumentException(String.format("Cannot truncate to a negative offset (%d).", targetOffset));
        if (targetOffset > logEndOffset()) {
            logger.info("Truncating {} to {} has no effect as the largest offset in the log is {}.", name(), targetOffset, logEndOffset() - 1);
            return;
        }
        synchronized (lock) {
            if (segments.firstEntry().getValue().baseOffset > targetOffset) {
                truncateFullyAndStartAt(targetOffset);
            } else {
                List<LogSegment> deletable = Utils.filter(logSegments(), new Predicate<LogSegment>() {
                    @Override
                    public boolean apply(LogSegment _) {
                        return _.baseOffset > targetOffset;
                    }
                });

                Utils.foreach(deletable, new Callable1<LogSegment>() {
                    @Override
                    public void apply(LogSegment _) {
                        deleteSegment(_);
                    }
                });
                activeSegment().truncateTo(targetOffset);
                this.nextOffset.set(targetOffset);
                this.recoveryPoint = Math.min(targetOffset, this.recoveryPoint);
            }
            truncates.getAndIncrement();
        }
    }

    /**
     * Delete all data in the log and start at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    public void truncateFullyAndStartAt(Long newOffset) {
        logger.debug("Truncate and start log '{}' to {}", name(), newOffset);
        synchronized (lock) {
            List<LogSegment> segmentsToDelete = Lists.newArrayList(logSegments());
            Utils.foreach(segmentsToDelete, new Callable1<LogSegment>() {
                @Override
                public void apply(LogSegment _) {
                    deleteSegment(_);
                }
            });
            addSegment(new LogSegment(dir,
                    newOffset,
                    config.indexInterval,
                    config.maxIndexSize,
                    time));
            this.nextOffset.set(newOffset);
            this.recoveryPoint = Math.min(newOffset, this.recoveryPoint);
            truncates.getAndIncrement();
        }
    }


    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public long lastFlushTime() {
        return lastflushedTime.get();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    public Collection<LogSegment> logSegments() {
        return segments.values();
    }

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset)
     */
    public Collection<LogSegment> logSegments(long from, long to) {
        synchronized (lock) {
            Long floor = segments.floorKey(from);
            if (floor == null)
                return segments.headMap(to).values();
            else
                return segments.subMap(floor, true, to, false).values();
        }
    }

    @Override
    public String toString() {
        return "Log(" + dir + ")";
    }

    /**
     * This method performs an asynchronous log segment delete by doing the following:
     * <ol>
     * <li>It removes the segment from the segment map so that it will no longer be used for reads.
     * <li>It renames the index and log files by appending .deleted to the respective file name
     * <li>It schedules an asynchronous delete operation to occur in the future
     * </ol>
     * This allows reads to happen concurrently without synchronization and without the possibility of physically
     * deleting a file while it is being read from.
     *
     * @param segment The log segment to schedule for deletion
     */
    private void deleteSegment(LogSegment segment) {
        logger.info("Scheduling log segment {} for log {} for deletion.", segment.baseOffset, name());
        synchronized (lock) {
            segments.remove(segment.baseOffset);
            asyncDeleteSegment(segment);
        }
    }

    /**
     * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
     *
     * @throws KafkaStorageException if the file can't be renamed and still exists
     */
    private void asyncDeleteSegment(final LogSegment segment) {
        segment.changeFileSuffixes("", Logs.DeletedFileSuffix);
        scheduler.schedule("delete-file", new Runnable() {
            @Override
            public void run() {
                logger.info("Deleting segment %d from log {}.", segment.baseOffset, name());
                segment.delete();
            }
        }, config.fileDeleteDelayMs);
    }

    /**
     * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
     * be asynchronously deleted.
     *
     * @param newSegment  The new log segment to add to the log
     * @param oldSegments The old log segments to delete from the log
     */
    public void replaceSegments(LogSegment newSegment, List<LogSegment> oldSegments, int expectedTruncates) {
        synchronized (lock) {
            if (expectedTruncates != numberOfTruncates())
                throw new OptimisticLockFailureException("The log has been truncated, expected %d but found %d.", expectedTruncates, numberOfTruncates());
            // need to do this in two phases to be crash safe AND do the delete asynchronously
            // if we crash in the middle of this we complete the swap in loadSegments()
            newSegment.changeFileSuffixes(Logs.CleanedFileSuffix, Logs.SwapFileSuffix);
            addSegment(newSegment);

            // delete the old files
            for (LogSegment seg : oldSegments) {
                // remove the index entry
                if (seg.baseOffset != newSegment.baseOffset)
                    segments.remove(seg.baseOffset);
                // delete segment
                asyncDeleteSegment(seg);
            }
            // okay we are safe now, remove the swap suffix
            newSegment.changeFileSuffixes(Logs.SwapFileSuffix, "");
        }
    }

    /**
     * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
     *
     * @param segment The segment to add
     */
    public LogSegment addSegment(LogSegment segment) {
        return this.segments.put(segment.baseOffset, segment);
    }

}
