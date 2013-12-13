package kafka.log;

import com.google.common.base.Throwables;
import kafka.common.InvalidOffsetException;
import kafka.utils.Function0;
import kafka.utils.Os;
import kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * <p/>
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * <p/>
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * <p/>
 * Index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * <p/>
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * <p/>
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * <p/>
 * The frequency of entries is up to the user of this class.
 * <p/>
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 */
public class OffsetIndex implements Closeable {
    public volatile File file;
    public long baseOffset;
    public int maxIndexSize;

    public OffsetIndex(File file, long baseOffset) {
        this(file, baseOffset, -1);
    }

    public OffsetIndex(File file, long baseOffset, int maxIndexSize) {
        this.file = file;
        this.baseOffset = baseOffset;
        this.maxIndexSize = maxIndexSize;

        mmap = loadMmap();
        size = new AtomicInteger(mmap.position() / 8);
        maxEntries = mmap.limit() / 8;
        lastOffset = readLastEntry().offset;

        logger.debug("Loaded index file {} with maxEntries = {}, maxIndexSize = {}, entries = {}, lastOffset = {}, file position = {}",
                file.getAbsolutePath(), maxEntries, maxIndexSize, entries(), lastOffset, mmap.position());

    }

    Logger logger = LoggerFactory.getLogger(OffsetIndex.class);

    private ReentrantLock lock = new ReentrantLock();

    /* initialize the memory mapping for this index */
    private MappedByteBuffer mmap;

    private MappedByteBuffer loadMmap() {
        RandomAccessFile raf = null;
        try {
            boolean newlyCreated = file.createNewFile();
            raf = new RandomAccessFile(file, "rw");
        /* pre-allocate the file if necessary */
            if (newlyCreated) {
                if (maxIndexSize < 8)
                    throw new IllegalArgumentException("Invalid max index size: " + maxIndexSize);
                raf.setLength(roundToExactMultiple(maxIndexSize, 8));
            }

        /* memory-map the file */
            long len = raf.length();
            MappedByteBuffer idx = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, len);

        /* set the position in the index for the next entry */
            if (newlyCreated)
                idx.position(0);
            else
                // if this is a pre-existing index, assume it is all valid and set position to last entry
                idx.position(roundToExactMultiple(idx.limit(), 8));

            return idx;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            Utils.closeQuietly(raf);
        }
    }


    /* the number of eight-byte entries currently in the index */
    private AtomicInteger size;

    /**
     * The maximum number of eight-byte entries this index can hold
     */
    public volatile int maxEntries;

    /* the last offset in the index */
    public long lastOffset;


    /**
     * The last entry in the index
     */
    public OffsetPosition readLastEntry() {
        return Utils.inLock(lock, new Function0<OffsetPosition>() {
            @Override
            public OffsetPosition apply() {
                int s = size.get();
                switch (s) {
                    case 0:
                        return new OffsetPosition(baseOffset, 0);
                    default:
                        return new OffsetPosition(baseOffset + relativeOffset(OffsetIndex.this.mmap, s - 1),
                                physical(OffsetIndex.this.mmap, s - 1));
                }
            }
        });
    }

    /**
     * Find the largest offset less than or equal to the given targetOffset
     * and return a pair holding this offset and it's corresponding physical file position.
     *
     * @param targetOffset The offset to look up.
     * @return The offset found and the corresponding file position for this offset.
     * If the target offset is smaller than the least entry in the index (or the index is empty),
     * the pair (baseOffset, 0) is returned.
     */
    public OffsetPosition lookup(final Long targetOffset) {
        return maybeLock(lock, new Function0<OffsetPosition>() {
            @Override
            public OffsetPosition apply() {
                ByteBuffer idx = mmap.duplicate();
                int slot = indexSlotFor(idx, targetOffset);
                if (slot == -1)
                    return new OffsetPosition(baseOffset, 0);
                else
                    return new OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot));
            }
        });
    }


    /**
     * Find the slot in which the largest offset less than or equal to the given
     * target offset is stored.
     *
     * @param idx          The index buffer
     * @param targetOffset The offset to look for
     * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
     */
    private int indexSlotFor(ByteBuffer idx, long targetOffset) {
        // we only store the difference from the base offset so calculate that
        long relOffset = targetOffset - baseOffset;

        // check if the index is empty
        if (entries() == 0)
            return -1;

        // check if the target offset is smaller than the least offset
        if (relativeOffset(idx, 0) > relOffset)
            return -1;

        // binary search for the entry
        int lo = 0;
        int hi = entries() - 1;
        while (lo < hi) {
            int mid = (int) Math.ceil(hi / 2.0 + lo / 2.0);
            int found = relativeOffset(idx, mid);
            if (found == relOffset)
                return mid;
            else if (found < relOffset)
                lo = mid;
            else
                hi = mid - 1;
        }
        return lo;
    }

    /* return the nth offset relative to the base offset */
    private int relativeOffset(ByteBuffer buffer, int n) {
        return buffer.getInt(n * 8);
    }

    /* return the nth physical position */
    private int physical(ByteBuffer buffer, int n) {
        return buffer.getInt(n * 8 + 4);
    }

    /**
     * Get the nth offset mapping from the index
     *
     * @param n The entry number in the index
     * @return The offset/position pair at that entry
     */
    public OffsetPosition entry(final int n) {
        return maybeLock(lock, new Function0<OffsetPosition>() {
            @Override
            public OffsetPosition apply() {
                if (n >= entries())
                    throw new IllegalArgumentException(String.format("Attempt to fetch the %dth entry from an index of size %d.", n, entries()));
                ByteBuffer idx = mmap.duplicate();
                return new OffsetPosition(relativeOffset(idx, n), physical(idx, n));
            }
        });
    }

    /**
     * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
     */
    public void append(final long offset, final int position) {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                checkState(!isFull(), "Attempt to append to a full index (size = " + size + ").");
                if (size.get() == 0 || offset > lastOffset) {
                    logger.debug("Adding index entry {} => {} to {}.", offset, position, file.getName());
                    mmap.putInt((int) (offset - baseOffset));
                    mmap.putInt(position);
                    size.incrementAndGet();
                    lastOffset = offset;
                    checkState(entries() * 8 == mmap.position(), entries() + " entries but file position in index is " + mmap.position() + ".");
                } else {
                    throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s.",
                            offset, entries(), lastOffset, file.getAbsolutePath());
                }
                return null;
            }
        });

    }

    /**
     * True iff there are no more slots available in this index
     */
    public boolean isFull() {
        return entries() >= this.maxEntries;
    }

    /**
     * Truncate the entire index, deleting all entries
     */
    public void truncate() {
        truncateToEntries(0);
    }

    /**
     * Remove all entries from the index which have an offset greater than or equal to the given offset.
     * Truncating to an offset larger than the largest in the index has no effect.
     */
    public void truncateTo(final long offset) {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                ByteBuffer idx = mmap.duplicate();
                int slot = indexSlotFor(idx, offset);

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
                int newEntries;
                if (slot < 0)
                    newEntries = 0;
                else if (relativeOffset(idx, slot) == offset - baseOffset)
                    newEntries = slot;
                else
                    newEntries = slot + 1;

                truncateToEntries(newEntries);

                return null;
            }
        });
    }


    /**
     * Truncates index to a known number of entries.
     */
    private void truncateToEntries(final int entries) {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                OffsetIndex.this.size.set(entries);
                mmap.position(OffsetIndex.this.size.get() * 8);
                OffsetIndex.this.lastOffset = readLastEntry().offset;
                return null;
            }
        });
    }

    /**
     * Trim this segment to fit just the valid entries, deleting all trailing unwritten bytes from
     * the file.
     */
    public void trimToValidSize() {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                resize(entries() * 8);
                return null;
            }
        });
    }

    /**
     * Reset the size of the memory map and the underneath file. This is used in two kinds of cases: (1) in
     * trimToValidSize() which is called at closing the segment or new segment being rolled; (2) at
     * loading segments from disk or truncating back to an old segment where a new log segment became active;
     * we want to reset the index size to maximum index size to avoid rolling new segment.
     */
    public void resize(final int newSize) {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                RandomAccessFile raf = null;

                try {
                    raf = new RandomAccessFile(file, "rws");
                    int roundedNewSize = roundToExactMultiple(newSize, 8);
                    int position = OffsetIndex.this.mmap.position();

      /* Windows won't let us modify the file length while the file is mmapped :-( */
                    if (Os.isWindows)
                        forceUnmap(OffsetIndex.this.mmap);

                    raf.setLength(roundedNewSize);
                    OffsetIndex.this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, roundedNewSize);
                    OffsetIndex.this.maxEntries = OffsetIndex.this.mmap.limit() / 8;
                    OffsetIndex.this.mmap.position(position);
                } catch (Throwable t) {
                    throw Throwables.propagate(t);
                } finally {
                    Utils.closeQuietly(raf);
                }
                return null;
            }
        });
    }

    /**
     * Forcefully free the buffer's mmap. We do this only on windows.
     */
    private void forceUnmap(MappedByteBuffer m) {
        try {
            if (m instanceof sun.nio.ch.DirectBuffer)
                ((sun.nio.ch.DirectBuffer) m).cleaner().clean();
        } catch (Throwable t) {
            logger.warn("Error when freeing index buffer", t);
        }
    }

    /**
     * Flush the data in the index to disk
     */
    public void flush() {
        Utils.inLock(lock, new Function0<Void>() {
            @Override
            public Void apply() {
                mmap.force();
                return null;
            }
        });
    }

    /**
     * Delete this index file
     */
    public boolean delete() {
        logger.info("Deleting index {}", this.file.getAbsolutePath());
        return this.file.delete();
    }

    /**
     * The number of entries in this index
     */
    public int entries() {
        return size.get();
    }

    /**
     * The number of bytes actually used by this index
     */
    public int sizeInBytes() {
        return 8 * entries();
    }

    /**
     * Close the index
     */
    public void close() {
        trimToValidSize();
    }

    /**
     * Rename the file that backs this offset index
     *
     * @return true iff the rename was successful
     */
    public boolean renameTo(File f) {
        boolean success = this.file.renameTo(f);
        this.file = f;
        return success;
    }

    /**
     * Do a basic sanity check on this index to detect obvious problems
     *
     * @throw IllegalArgumentException if any problems are found
     */
    public void sanityCheck() {
        checkState(entries() == 0 || lastOffset > baseOffset,
                String.format("Corrupt index found, index file (%s) has non-zero size but the last offset is %d and the base offset is %d",
                        file.getAbsolutePath(), lastOffset, baseOffset));
        long len = file.length();
        checkState(len % 8 == 0,
                "Index file " + file.getName() + " is corrupt, found " + len +
                        " bytes which is not positive or not a multiple of 8.");
    }

    /**
     * Round a number to the greatest exact multiple of the given factor less than the given number.
     * E.g. roundToExactMultiple(67, 8) == 64
     */
    private int roundToExactMultiple(int number, int factor) {
        return factor * (number / factor);
    }

    /**
     * Execute the given function in a lock only if we are running on windows. We do this
     * because Windows won't let us resize a file while it is mmapped. As a result we have to force unmap it
     * and this requires synchronizing reads.
     */
    public <T> T maybeLock(Lock lock, Function0<T> fun) {
        if (Os.isWindows)
            lock.lock();
        try {
            return fun.apply();
        } finally {
            if (Os.isWindows)
                lock.unlock();
        }
    }

}
