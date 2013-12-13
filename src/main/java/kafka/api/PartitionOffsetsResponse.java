package kafka.api;

import kafka.common.ErrorMapping;

import java.util.List;

public class PartitionOffsetsResponse {
    public short error;
    public List<Long> offsets;

    public PartitionOffsetsResponse(short error, List<Long> offsets) {
        this.error = error;
        this.offsets = offsets;
    }

    @Override
    public String toString() {
        return "error: " + ErrorMapping.exceptionFor(error).getClass().getName() + " offsets: " + offsets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionOffsetsResponse that = (PartitionOffsetsResponse) o;

        if (error != that.error) return false;
        if (offsets != null ? !offsets.equals(that.offsets) : that.offsets != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) error;
        result = 31 * result + (offsets != null ? offsets.hashCode() : 0);
        return result;
    }
}
