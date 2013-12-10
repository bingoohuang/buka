package kafka.message;

import kafka.common.KafkaException;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class CompressionFactory {
    public static OutputStream apply(CompressionCodec compressionCodec, OutputStream stream) {
        try {
            if (compressionCodec == DefaultCompressionCodec.instance) return new GZIPOutputStream(stream);
            if (compressionCodec == GZIPCompressionCodec.instance) return new GZIPOutputStream(stream);
            if (compressionCodec == SnappyCompressionCodec.instance) return new SnappyOutputStream(stream);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }

    public static InputStream apply(CompressionCodec compressionCodec, InputStream stream) {
        try {
            if (compressionCodec == DefaultCompressionCodec.instance) return new GZIPInputStream(stream);
            if (compressionCodec == GZIPCompressionCodec.instance) return new GZIPInputStream(stream);
            if (compressionCodec == SnappyCompressionCodec.instance) return new SnappyInputStream(stream);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        throw new kafka.common.UnknownCodecException("Unknown Codec: " + compressionCodec);
    }
}
