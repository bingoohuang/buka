package kafka.common;

import kafka.utils.Tuple3;

/**
 * Convenience case class since (topic, partition) pairs are ubiquitous.
 */
public class OffsetMetadataAndError {
    public static final long InvalidOffset = -1L;
    public static final String NoMetadata = "";


    public long offset;
    public String metadata;
    public short error;

    public OffsetMetadataAndError(long offset) {
        this(offset, NoMetadata, ErrorMapping.NoError);
    }

    public OffsetMetadataAndError(long offset, String metadata) {
        this(offset, metadata, ErrorMapping.NoError);
    }
    public OffsetMetadataAndError(long offset, String metadata, short error) {
        this.offset = offset;
        this.metadata = metadata;
        this.error = error;
    }

    public OffsetMetadataAndError(Tuple3<Long, String, Short> tuple) {
        this(tuple._1, tuple._2, tuple._3);
    }

    public Tuple3<Long, String, Short> asTuple() {
        return Tuple3.make(offset, metadata, error);
    }

    @Override
    public String toString() {
        return String.format("OffsetAndMetadata[%d,%s,%d]", offset, metadata, error);
    }
}
