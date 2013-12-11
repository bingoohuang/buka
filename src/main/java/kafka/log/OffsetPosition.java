package kafka.log;

/**
 * The mapping between a logical log offset and the physical position
 * in some log file of the beginning of the message set entry with the
 * given offset.
 */
public class OffsetPosition {
    public final long offset;
    public final int position;

    public OffsetPosition(long offset, int position) {
        this.offset = offset;
        this.position = position;
    }

    @Override
    public String toString() {
        return "OffsetPosition{" +
                "offset=" + offset +
                ", position=" + position +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetPosition that = (OffsetPosition) o;

        if (offset != that.offset) return false;
        if (position != that.position) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + position;
        return result;
    }
}
