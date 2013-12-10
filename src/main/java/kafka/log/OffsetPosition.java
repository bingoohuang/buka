package kafka.log;

/**
 * The mapping between a logical log offset and the physical position
 * in some log file of the beginning of the message set entry with the
 * given offset.
 */
public class OffsetPosition {
    public final long offset;
    public final  int position;

    public OffsetPosition(long offset, int position) {
        this.offset = offset;
        this.position = position;
    }
}
