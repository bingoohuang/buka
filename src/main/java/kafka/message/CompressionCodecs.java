package kafka.message;


public abstract class CompressionCodecs {
    public static CompressionCodec getCompressionCodec(int codec) {
        if (codec == NoCompressionCodec.instance.codec()) return NoCompressionCodec.instance;
        if (codec == GZIPCompressionCodec.instance.codec()) return GZIPCompressionCodec.instance;
        if (codec == SnappyCompressionCodec.instance.codec()) return SnappyCompressionCodec.instance;

        throw new kafka.common.UnknownCodecException("%d is an unknown compression codec", codec);
    }


    public static CompressionCodec getCompressionCodec(String name) {
        String lowercaseName = name.toLowerCase();
        if (NoCompressionCodec.instance.name().equals(lowercaseName)) return NoCompressionCodec.instance;
        if (GZIPCompressionCodec.instance.name().equals(lowercaseName)) return GZIPCompressionCodec.instance;
        if (SnappyCompressionCodec.instance.name().equals(lowercaseName)) return SnappyCompressionCodec.instance;

        throw new kafka.common.UnknownCodecException("%s is an unknown compression codec", name);
    }

}
