package kafka.api;

public class ProducerResponseStatus {
    public short error;
    public long offset;

    public ProducerResponseStatus(short error, long offset) {
        this.error = error;
        this.offset = offset;
    }
}
