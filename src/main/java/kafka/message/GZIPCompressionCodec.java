package kafka.message;

public class GZIPCompressionCodec implements CompressionCodec {
    public static final CompressionCodec instance = new GZIPCompressionCodec();

    @Override
    public int codec() {
        return 1;
    }

    @Override
    public String name() {
        return "gzip";
    }
}
