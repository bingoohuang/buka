package kafka.message;


public class DefaultCompressionCodec implements CompressionCodec {
    public static final CompressionCodec instance = new DefaultCompressionCodec();

    @Override
    public int codec() {
        return GZIPCompressionCodec.instance.codec();
    }

    @Override
    public String name() {
        return GZIPCompressionCodec.instance.name();
    }
}
