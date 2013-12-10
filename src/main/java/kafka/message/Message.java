package kafka.message;

import kafka.common.InvalidMessageException;
import kafka.utils.Utils;

import java.nio.ByteBuffer;

/**
 * A message. The format of an N byte message is the following:
 * <p/>
 * 1. 4 byte CRC32 of the message
 * 2. 1 byte "magic" identifier to allow format changes, value is 2 currently
 * 3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
 * 4. 4 byte key length, containing length K
 * 5. K byte key
 * 6. 4 byte payload length, containing length V
 * 7. V byte payload
 * <p/>
 * Default constructor wraps an existing ByteBuffer with the Message object with no change to the contents.
 */
public class Message {
    public final ByteBuffer buffer;

    public Message(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * A constructor to create a Message
     *
     * @param bytes         The payload of the message
     * @param codec         The compression codec used on the contents of the message (if any)
     * @param key           The key of the message (null, if none)
     * @param payloadOffset The offset into the payload array used to extract payload
     * @param payloadSize   The size of the payload to use
     */
    public Message(byte[] bytes, byte[] key, CompressionCodec codec, int payloadOffset, int payloadSize) {
        this(ByteBuffer.allocate(Messages.CrcLength +
                Messages.MagicLength +
                Messages.AttributesLength +
                Messages.KeySizeLength +
                (key == null ? 0 : key.length) +
                Messages.ValueSizeLength +
                (bytes == null ? 0 : (payloadSize >= 0 ? payloadSize : bytes.length - payloadOffset))));

        // skip crc, we will fill that in at the end
        buffer.position(Messages.MagicOffset);
        buffer.put(Messages.CurrentMagicValue);
        byte attributes = 0;
        if (codec.codec() > 0)
            attributes = (byte) (attributes | (Messages.CompressionCodeMask & codec.codec()));
        buffer.put(attributes);
        if (key == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(key.length);
            buffer.put(key, 0, key.length);
        }
        final int size = (bytes == null) ? -1 :
                (payloadSize >= 0 ? payloadSize
                        : bytes.length - payloadOffset);

        buffer.putInt(size);
        if (bytes != null)
            buffer.put(bytes, payloadOffset, size);
        buffer.rewind();

        // now compute the checksum and fill it in
        Utils.writeUnsignedInt(buffer, Messages.CrcOffset, computeChecksum());
    }

    public Message(byte[] bytes, byte[] key, CompressionCodec codec) {
        this(bytes = bytes, key = key, codec = codec, /*payloadOffset =*/ 0, /*payloadSize =*/ -1);
    }

    public Message(byte[] bytes, CompressionCodec codec) {
        this(bytes = bytes, /*key = */null, codec = codec);
    }

    public Message(byte[] bytes, byte[] key) {
        this(bytes = bytes, /*key = */key, /*codec = */NoCompressionCodec.instance);
    }

    public Message(byte[] bytes) {
        this(bytes = bytes, /*key = */null, /*codec = */NoCompressionCodec.instance);
    }

    /**
     * Compute the checksum of the message from the message contents
     */
    public long computeChecksum() {
        return Utils.crc32(buffer.array(), buffer.arrayOffset() + Messages.MagicOffset,
                buffer.limit() - Messages.MagicOffset);
    }

    /**
     * Retrieve the previously computed CRC for this message
     */
    public long checksum() {
        return Utils.readUnsignedInt(buffer, Messages.CrcOffset);
    }

    /**
     * Returns true if the crc stored with the message matches the crc computed off the message contents
     */
    public boolean isValid() {
        return checksum() == computeChecksum();
    }

    /**
     * Throw an InvalidMessageException if isValid is false for this message
     */
    public void ensureValid() {
        if (!isValid())
            throw new InvalidMessageException(
                    "Message is corrupt (stored crc = %d, computed crc = %d)", checksum(), computeChecksum());
    }

    /**
     * The complete serialized size of this message in bytes (including crc, header attributes, etc)
     */
    public int size() {
        return buffer.limit();
    }

    /**
     * The length of the key in bytes
     */
    public int keySize() {
        return buffer.getInt(Messages.KeySizeOffset);
    }

    /**
     * Does the message have a key?
     */
    public boolean hasKey() {
        return keySize() >= 0;
    }

    /**
     * The position where the payload size is stored
     */
    private int payloadSizeOffset() {
        return Messages.KeyOffset + Math.max(0, keySize());
    }

    /**
     * The length of the message value in bytes
     */
    public int payloadSize() {
        return buffer.getInt(payloadSizeOffset());
    }

    /**
     * Is the payload of this message null
     */
    public boolean isNull() {
        return payloadSize() < 0;
    }

    /**
     * The magic version of this message
     */
    public byte magic() {
        return buffer.get(Messages.MagicOffset);
    }

    /**
     * The attributes stored with this message
     */
    public byte attributes() {
        return buffer.get(Messages.AttributesOffset);
    }

    /**
     * The compression codec used with this message
     */
    public CompressionCodec compressionCodec() {
        return CompressionCodecs.getCompressionCodec(buffer.get(Messages.AttributesOffset) & Messages.CompressionCodeMask);
    }

    /**
     * A ByteBuffer containing the content of the message
     */
    public ByteBuffer payload() {
        return sliceDelimited(payloadSizeOffset());
    }

    /**
     * A ByteBuffer containing the message key
     */
    public ByteBuffer key() {
        return sliceDelimited(Messages.KeySizeOffset);
    }

    /**
     * Read a size-delimited byte buffer starting at the given offset
     */
    private ByteBuffer sliceDelimited(int start) {
        int size = buffer.getInt(start);
        if (size < 0) {
            return null;
        } else {
            ByteBuffer b = buffer.duplicate();
            b.position(start + 4);
            b = b.slice();
            b.limit(size);
            b.rewind();
            return b;
        }
    }

    public String toString() {
        return String.format(
                "Message(magic = %d, attributes = %d, crc = %d, key = %s, payload = %s)",
                magic(), attributes(), checksum(), key(), payload());
    }

    @Override
    public boolean equals(Object any) {
        if (any instanceof Message) {
            return buffer.equals(((Message) any).buffer);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }
}
