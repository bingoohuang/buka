package kafka.message;


public class SnappyCompressionCodec implements CompressionCodec {
    public static final CompressionCodec instance = new SnappyCompressionCodec();

    @Override
    public int codec() {
        return 2;
    }

    @Override
    public String name() {
        return "snappy";
    }
}
