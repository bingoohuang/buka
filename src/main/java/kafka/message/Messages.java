package kafka.message;

/**
 * Constants related to messages
 */
public interface Messages {
    /**
     * The current offset and size for all the fixed-length fields
     */
    int CrcOffset = 0;
    int CrcLength = 4;
    int MagicOffset = CrcOffset + CrcLength;
    int MagicLength = 1;
    int AttributesOffset = MagicOffset + MagicLength;
    int AttributesLength = 1;
    int KeySizeOffset = AttributesOffset + AttributesLength;
    int KeySizeLength = 4;
    int KeyOffset = KeySizeOffset + KeySizeLength;
    int ValueSizeLength = 4;

    /** The amount of overhead bytes in a message */
    int MessageOverhead = KeyOffset + ValueSizeLength;

    /**
     * The minimum valid size for the message header
     */
    int MinHeaderSize = CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength;

    /**
     * The current "magic" value
     */
    byte CurrentMagicValue = 0;

    /**
     * Specifies the mask for the compression code. 2 bits to hold the compression codec.
     * 0 is reserved to indicate no compression
     */
    int CompressionCodeMask = 0x03;

    /**
     * Compression code for uncompressed messages
     */
    int NoCompression = 0;
}
