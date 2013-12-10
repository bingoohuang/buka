package kafka.serializer;

import kafka.common.KafkaException;
import kafka.utils.VerifiableProperties;

import java.io.UnsupportedEncodingException;

/**
 * The string encoder translates strings into bytes. It uses UTF8 by default but takes
 * an optional property serializer.encoding to control this.
 */
public class StringDecoder implements Decoder<String> {
    public final VerifiableProperties props;
    private final String encoding;

    public StringDecoder() {
        this(null);
    }

    public StringDecoder(VerifiableProperties props) {
        this.props = props;

        encoding = (props == null) ? "UTF8" : props.getString("serializer.encoding", "UTF8");
    }


    @Override
    public String fromBytes(byte[] bytes) {
        try {
            return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }
}
