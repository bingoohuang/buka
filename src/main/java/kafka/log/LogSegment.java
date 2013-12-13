package kafka.log;

import kafka.common.KafkaStorageException;
import kafka.message.*;
import kafka.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;

@NonThreadSafe
public class LogSegment {
    public FileMessageSet log;
    public OffsetIndex index;
    public long baseOffset;
    public int indexIntervalBytes;
    public Time time;

    /**
     * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
     * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each
     * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
     * any previous segment.
     * <p/>
     * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file.
     *
     * @param log                The message set containing log entries
     * @param index              The offset index
     * @param baseOffset         A lower bound on the offsets in this segment
     * @param indexIntervalBytes The approximate number of bytes between entries in the index
     * @param time               The time instance
     */
    public LogSegment(FileMessageSet log, OffsetIndex index, long baseOffset, int indexIntervalBytes, Time time) {
        this.log = log;
        this.index = index;
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.time = time;

        created = time.milliseconds();
    }

    public long created;

    /* the number of bytes since we last added an entry in the offset index */
    private int bytesSinceLastIndexEntry = 0;

    public LogSegment(File dir, Long startOffset, int indexIntervalBytes, int maxIndexSize, Time time) {
        this(new FileMessageSet(Logs.logFilename(dir, startOffset)),
                new OffsetIndex(Logs.indexFilename(dir, startOffset), startOffset, maxIndexSize),
                startOffset,
                indexIntervalBytes,
                time);
    }

    Logger logger = LoggerFactory.getLogger(LogSegment.class);

    /* Return the size in bytes of this log segment */
    public long size() {
        return log.sizeInBytes();
    }

    /**
     * Append the given messages starting with the given offset. Add
     * an entry to the index if needed.
     * <p/>
     * It is assumed this method is being called from within a lock.
     *
     * @param offset   The first offset in the message set.
     * @param messages The messages to append.
     */
    @NonThreadSafe
    public void append(long offset, ByteBufferMessageSet messages) {
        if (messages.sizeInBytes() > 0) {
            logger.trace("Inserting {} bytes at offset {} at position {}", messages.sizeInBytes(), offset, log.sizeInBytes());
            // append an entry to the index (if needed)
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                index.append(offset, log.sizeInBytes());
                this.bytesSinceLastIndexEntry = 0;
            }
            // append the messages
            log.append(messages);
            this.bytesSinceLastIndexEntry += messages.sizeInBytes();
        }
    }

    /**
     * Find the physical file position for the first message with offset >= the requested offset.
     * <p/>
     * The lowerBound argument is an optimization that can be used if we already know a valid starting position
     * in the file higher than the greast-lower-bound from the index.
     *
     * @param offset               The offset we want to translate
     * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
     *                             when omitted, the search will begin at the position in the offset index.
     * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
     */
    @ThreadSafe
    private OffsetPosition translateOffset(long offset, int startingFilePosition /*= 0*/) {
        OffsetPosition mapping = index.lookup(offset);
        return log.searchFor(offset, Math.max(mapping.position, startingFilePosition));
    }

    @ThreadSafe
    private OffsetPosition translateOffset(long offset) {
        return translateOffset(offset, 0);
    }

    /**
     * Read a message set from this segment beginning with the first offset >= startOffset. The message set will include
     * no more than maxSize bytes and will end before maxOffset if a maxOffset is specified.
     *
     * @param startOffset A lower bound on the first offset to include in the message set we read
     * @param maxSize     The maximum number of bytes to include in the message set we read
     * @param maxOffset   An optional maximum offset for the message set we read
     * @return The message set read or null if the startOffset is larger than the largest offset in this log.
     */
    @ThreadSafe
    public MessageSet read(long startOffset, Long maxOffset, int maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException(String.format("Invalid max size for log read (%d)", maxSize));
        if (maxSize == 0)
            return MessageSets.Empty;

        int logSize = log.sizeInBytes(); // this may change, need to save a consistent copy
        OffsetPosition startPosition = translateOffset(startOffset);

        // if the start position is already off the end of the log, return null
        if (startPosition == null) return null;

        // calculate the length of the message set to read based on whether or not they gave us a maxOffset
        int length;
        if (maxOffset == null) length = maxSize; // no max offset, just use the max size they gave unmolested
        else {
            // there is a max offset, translate it to a file position and use that to calculate the max read size
            if (maxOffset < startOffset)
                throw new IllegalArgumentException(String.format("Attempt to read with a maximum offset (%d) less than the start offset (%d).", maxOffset, startOffset));
            OffsetPosition mapping = translateOffset(maxOffset, startPosition.position);
            int endPosition =
                    mapping == null
                            ? logSize // the max offset is off the end of the log, use the end of the file
                            : mapping.position;
            length = Math.min(endPosition - startPosition.position, maxSize);
        }

        return log.read(startPosition.position, length);
    }

    /**
     * Run recovery on the given segment. This will rebuild the index from the log file and lop off any invalid bytes from the end of the log and index.
     *
     * @param maxMessageSize A bound the memory allocation in the case of a corrupt message size--we will assume any message larger than this
     *                       is corrupt.
     * @return The number of bytes truncated from the log
     */
    @NonThreadSafe
    public int recover(int maxMessageSize) {
        index.truncate();
        index.resize(index.maxIndexSize);
        int validBytes = 0;
        int lastIndexEntry = 0;
        Iterator<MessageAndOffset> iter = log.iterator(maxMessageSize);
        try {
            while (iter.hasNext()) {
                MessageAndOffset entry = iter.next();
                entry.message.ensureValid();
                if (validBytes - lastIndexEntry > indexIntervalBytes) {
                    // we need to decompress the message, if required, to get the offset of the first uncompressed message
                    long startOffset;

                    if (entry.message.compressionCodec() == NoCompressionCodec.instance) {
                        startOffset = entry.offset;
                    } else {
                        startOffset = Utils.head(ByteBufferMessageSets.decompress(entry.message)).offset;
                    }
                    index.append(startOffset, validBytes);
                    lastIndexEntry = validBytes;
                }
                validBytes += MessageSets.entrySize(entry.message);
            }
        } catch (InvalidMessageException e) {
            logger.warn("Found invalid messages in log segment {} at byte offset {}: {}.", log.file.getAbsolutePath(), validBytes, e.getMessage());
        }
        int truncated = log.sizeInBytes() - validBytes;
        log.truncateTo(validBytes);
        index.trimToValidSize();
        return truncated;
    }

    @Override
    public String toString() {
        return "LogSegment(baseOffset=" + baseOffset + ", size=" + size() + ")";
    }

    /**
     * Truncate off all index and log entries with offsets >= the given offset.
     * If the given offset is larger than the largest message in this segment, do nothing.
     *
     * @param offset The offset to truncate to
     * @return The number of log bytes truncated
     */
    @NonThreadSafe
    public int truncateTo(long offset) {
        OffsetPosition mapping = translateOffset(offset);
        if (mapping == null)
            return 0;
        index.truncateTo(offset);
        // after truncation, reset and allocate more space for the (new currently  active) index
        index.resize(index.maxIndexSize);
        int bytesTruncated = log.truncateTo(mapping.position);
        if (log.sizeInBytes() == 0)
            created = time.milliseconds();
        bytesSinceLastIndexEntry = 0;
        return bytesTruncated;
    }

    /**
     * Calculate the offset that would be used for the next message to be append to this segment.
     * Note that this is expensive.
     */
    @ThreadSafe
    public long nextOffset() {
        MessageSet ms = read(index.lastOffset, null, log.sizeInBytes());
        if (ms == null) {
            return baseOffset;
        } else {
            MessageAndOffset last = Utils.lastOption(ms);
            if (last == null) return baseOffset;
            return last.nextOffset();
        }
    }

    /**
     * Flush this log segment to disk
     */
    @ThreadSafe
    public void flush() {
        LogFlushStats.instance.logFlushTimer.time(new Function0<Void>() {
            @Override
            public Void apply() {
                log.flush();
                index.flush();
                return null;
            }
        });
    }

    /**
     * Change the suffix for the index and log file for this log segment
     */
    public void changeFileSuffixes(String oldSuffix, String newSuffix) {
        boolean logRenamed = log.renameTo(new File(Utils.replaceSuffix(log.file.getPath(), oldSuffix, newSuffix)));
        if (!logRenamed)
            throw new KafkaStorageException("Failed to change the log file suffix from %s to %s for log segment %d", oldSuffix, newSuffix, baseOffset);
        boolean indexRenamed = index.renameTo(new File(Utils.replaceSuffix(index.file.getPath(), oldSuffix, newSuffix)));
        if (!indexRenamed)
            throw new KafkaStorageException("Failed to change the index file suffix from %s to %s for log segment %d", oldSuffix, newSuffix, baseOffset);
    }

    /**
     * Close this log segment
     */
    public void close() {
        Utils.closeQuietly(index);
        Utils.closeQuietly(log);
    }

    /**
     * Delete this log segment from the filesystem.
     *
     * @throws KafkaStorageException if the delete fails.
     */
    public void delete() {
        boolean deletedLog = log.delete();
        boolean deletedIndex = index.delete();
        if (!deletedLog && log.file.exists())
            throw new KafkaStorageException("Delete of log " + log.file.getName() + " failed.");
        if (!deletedIndex && index.file.exists())
            throw new KafkaStorageException("Delete of index " + index.file.getName() + " failed.");
    }

    /**
     * The last modified time of this log segment as a unix time stamp
     */
    public long lastModified() {
        return log.file.lastModified();
    }

    /**
     * Change the last modified time for this log segment
     */
    public void lastModified(long ms) {
        log.file.setLastModified(ms);
        index.file.setLastModified(ms);
    }
}
