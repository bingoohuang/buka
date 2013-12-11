package kafka.api;

import kafka.common.KafkaException;
import kafka.utils.Range;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Helper functions specific to parsing or serializing requests and responses
 */
public abstract class ApiUtils {
    public static final String ProtocolEncoding = "UTF-8";

    /**
     * Read size prefixed string where the size is stored as a 2 byte short.
     *
     * @param buffer The buffer to read from
     */
    public static String readShortString(ByteBuffer buffer) {
        int size = buffer.getShort();
        if (size < 0) return null;

        byte[] bytes = new byte[size];
        buffer.get(bytes);

        return getString(bytes);
    }

    private static String getString(byte[] bytes) {
        try {
            return new String(bytes, ProtocolEncoding);
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Write a size prefixed string where the size is stored as a 2 byte short
     *
     * @param buffer The buffer to write to
     * @param string The string to write
     */
    public static void writeShortString(ByteBuffer buffer, String string) {
        if (string == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] encodedString = getBytes(string);
            if (encodedString.length > Short.MAX_VALUE) {
                throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");
            } else {
                buffer.putShort((short) encodedString.length);
                buffer.put(encodedString);
            }
        }
    }

    private static byte[] getBytes(String string) {
        try {
            return string.getBytes(ProtocolEncoding);
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Return size of a size prefixed string where the size is stored as a 2 byte short
     *
     * @param string The string to write
     */
    public static int shortStringLength(String string) {
        if (string == null) return 2;

        byte[] encodedString = getBytes(string);
        if (encodedString.length > Short.MAX_VALUE)
            throw new KafkaException("String exceeds the maximum size of " + Short.MAX_VALUE + ".");

        return 2 + encodedString.length;
    }

    /**
     * Read an integer out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
    public static int readIntInRange(ByteBuffer buffer, String name, Range<Integer> range) {
        int value = buffer.getInt();
        if (value < range._1 || value > range._2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");

        return value;
    }

    /**
     * Read a short out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
    public static short readShortInRange(ByteBuffer buffer, String name, Range<Short> range) {
        short value = buffer.getShort();
        if (value < range._1 || value > range._2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");

        return value;
    }

    /**
     * Read a long out of the bytebuffer from the current position and check that it falls within the given
     * range. If not, throw KafkaException.
     */
    public static long readLongInRange(ByteBuffer buffer, String name, Range<Long> range) {
        long value = buffer.getLong();
        if (value < range._1 || value > range._2)
            throw new KafkaException(name + " has value " + value + " which is not in the range " + range + ".");

        return value;
    }
}
