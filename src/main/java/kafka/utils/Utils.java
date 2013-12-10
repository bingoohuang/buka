package kafka.utils;

import com.google.common.collect.Iterables;
import kafka.common.KafkaException;
import kafka.common.KafkaStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * General helper functions!
 * <p/>
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 * <p/>
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
public abstract class Utils {
    static Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Create a daemon thread
     *
     * @param runnable The runnable to execute in the background
     * @return The unstarted thread
     */
    public static Thread daemonThread(Runnable runnable) {
        return newThread(runnable, true);
    }

    /**
     * Create a daemon thread
     *
     * @param name     The name of the thread
     * @param runnable The runnable to execute in the background
     * @return The unstarted thread
     */
    public static Thread daemonThread(String name, Runnable runnable) {
        return newThread(name, runnable, true);
    }

    /**
     * Create a new thread
     *
     * @param name     The name of the thread
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(String name, Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread '{}':", t.getName(), e);
            }
        });

        return thread;
    }

    /**
     * Create a new thread
     *
     * @param runnable The work for the thread to do
     * @param daemon   Should the thread block JVM shutdown?
     * @return The unstarted thread
     */
    public static Thread newThread(Runnable runnable, boolean daemon) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught exception in thread '{}':", t.getName(), e);
            }
        });

        return thread;
    }

    /**
     * Read the given byte buffer into a byte array
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        return readBytes(buffer, 0, buffer.limit());
    }

    /**
     * Read a byte array from the given offset and size in the buffer
     */
    public static byte[] readBytes(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            buffer.mark();
            buffer.get(dest);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Read a properties file from the given path
     *
     * @param filename The path of the file to read
     */
    public static Properties loadProps(String filename) {
        FileInputStream propStream = null;
        Properties props = new Properties();
        try {
            propStream = new FileInputStream(filename);
            props.load(propStream);
        } catch (IOException e) {
            throw new KafkaException(e, "error load props %s", filename);
        } finally {
            closeQuietly(propStream);
        }

        return props;
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            logger.warn("try to close {} error", closeable, e);
        }
    }

    /**
     * Open a channel for the given file
     */
    public static FileChannel openChannel(File file, boolean mutable) {
        try {
            return mutable ? new RandomAccessFile(file, "rw").getChannel()
                    : new FileInputStream(file).getChannel();
        } catch (FileNotFoundException e) {
            throw new KafkaException(e, "try to openChannel %s error", file, e);
        }
    }

    /**
     * Do the given action and log any exceptions thrown without rethrowing them
     *
     * @param log    The log method to use for logging. E.g. logger.warn
     * @param action The action to execute
     */
    public static void swallow(Function2<Object, Throwable, Void> log, Function<Void> action) {
        try {
            action.apply();
        } catch (Throwable e) {
            log.apply(e.getMessage(), e);
        }
    }

    /**
     * Test if two byte buffers are equal. In this case equality means having
     * the same bytes from the current position to the limit
     */
    public static boolean equal(ByteBuffer b1, ByteBuffer b2) {
        // two byte buffers are equal if their position is the same,
        // their remaining bytes are the same, and their contents are the same
        if (b1.position() != b2.position()) return false;
        if (b1.remaining() != b2.remaining()) return false;
        for (int i = 0, ii = b1.remaining(); i < ii; ++i)
            if (b1.get(i) != b2.get(i)) return false;
        return true;
    }

    /**
     * Translate the given buffer into a string
     *
     * @param buffer The buffer to translate
     */
    public static String readString(ByteBuffer buffer) {
        return Charset.defaultCharset().toString();
    }

    /**
     * Translate the given buffer into a string
     *
     * @param buffer   The buffer to translate
     * @param encoding The encoding to use in translating bytes to characters
     */
    public static String readString(ByteBuffer buffer, String encoding) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        try {
            return new String(bytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new KafkaException(e, "readString error");
        }
    }

    /**
     * Print an error message and shutdown the JVM
     *
     * @param message The error message
     */
    public static void croak(String message) {
        System.err.println(message);
        System.exit(1);
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(String file) {
        rm(new File(file));
    }

    /**
     * Recursively delete the list of files/directories and any subfiles (if any exist)
     *
     * @param files sequence of files to be deleted
     */
    public static void rm(List<String> files) {
        for (String f : files) rm(new File(f));
    }

    /**
     * Recursively delete the given file/directory and any subfiles (if any exist)
     *
     * @param file The root file at which to begin deleting
     */
    public static void rm(File file) {
        if (file == null) {
            return;
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files)
                rm(f);

            file.delete();
        } else {
            file.delete();
        }
    }

    /**
     * Register the given mbean with the platform mbean server,
     * unregistering any mbean that was there before. Note,
     * this method will not throw an exception if the registration
     * fails (since there is nothing you can do and it isn't fatal),
     * instead it just returns false indicating the registration failed.
     *
     * @param mbean The object to register as an mbean
     * @param name  The name to register this mbean with
     * @return true if the registration succeeded
     */
    public static boolean registerMBean(Object mbean, String name) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName))
                    mbs.unregisterMBean(objName);
                mbs.registerMBean(mbean, objName);
                return true;
            }
        } catch (Exception e) {
            logger.error("Failed to register Mbean {}", name, e);
            return false;
        }
    }

    /**
     * Unregister the mbean with the given name, if there is one registered
     *
     * @param name The mbean name to unregister
     */
    public static void unregisterMBean(String name) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            synchronized (mbs) {
                ObjectName objName = new ObjectName(name);
                if (mbs.isRegistered(objName))
                    mbs.unregisterMBean(objName);
            }
        } catch (Exception e) {
            logger.error("Failed to unregister Mbean {}", name, e);
        }
    }

    /**
     * Read an unsigned integer from the current position in the buffer,
     * incrementing the position by 4 bytes
     *
     * @param buffer The buffer to read from
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer) {
        return buffer.getInt() & 0xffffffffL;
    }

    /**
     * Read an unsigned integer from the given position without modifying the buffers
     * position
     *
     * @param buffer the buffer to read from
     * @param index  the index from which to read the integer
     * @return The integer read, as a long to avoid signedness
     */
    public static long readUnsignedInt(ByteBuffer buffer, int index) {
        return buffer.getInt(index) & 0xffffffffL;
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param value  The value to write
     */
    public static void writetUnsignedInt(ByteBuffer buffer, long value) {
        buffer.putInt((int) (value & 0xffffffffL));
    }

    /**
     * Write the given long value as a 4 byte unsigned integer. Overflow is ignored.
     *
     * @param buffer The buffer to write to
     * @param index  The position in the buffer at which to begin writing
     * @param value  The value to write
     */
    public static void writeUnsignedInt(ByteBuffer buffer, int index, long value) {
        buffer.putInt(index, (int) (value & 0xffffffffL));
    }

    /**
     * Compute the CRC32 of the byte array
     *
     * @param bytes The array to compute the checksum for
     * @return The CRC32
     */
    public long crc32(byte[] bytes) {
        return crc32(bytes, 0, bytes.length);
    }

    /**
     * Compute the CRC32 of the segment of the byte array given by the specificed size and offset
     *
     * @param bytes  The bytes to checksum
     * @param offset the offset at which to begin checksumming
     * @param size   the number of bytes to checksum
     * @return The CRC32
     */
    public static long crc32(byte[] bytes, int offset, int size) {
        Crc32 crc = new Crc32();
        crc.update(bytes, offset, size);
        return crc.getValue();
    }

    /**
     * Compute the hash code for the given items
     */
    public static int hashcode(Object... as) {
        if (as == null)
            return 0;
        int h = 1;
        int i = 0;
        while (i < as.length) {
            if (as[i] != null) {
                h = 31 * h + as[i].hashCode();
                i += 1;
            }
        }
        return h;
    }

    /**
     * Group the given values by keys extracted with the given function
     */
    public static <K, V> Map<K, List<V>> groupby(Iterable<V> vals, Function1<V, K> f) {
        Map<K, List<V>> m = new HashMap<K, List<V>>();
        for (V v : vals) {
            K k = f.apply(v);
            List<V> vs = m.get(k);
            if (vs != null) vs.add(0, v);
            else {
                vs = new LinkedList<V>();
                vs.add(v);
                m.put(k, vs);
            }
        }

        return m;
    }

    /**
     * Read some bytes into the provided buffer, and return the number of bytes read. If the
     * channel has been closed or we get -1 on the read for any reason, throw an EOFException
     */
    public static int read(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {
        int read = channel.read(buffer);
        if (read == -1) throw new EOFException("Received -1 when reading from channel, socket has likely been closed.");

        return read;
    }

    /**
     * Throw an exception if the given value is null, else return it. You can use this like:
     * val myValue = Utils.notNull(expressionThatShouldntBeNull)
     */
    public static <V> V notNull(V v) {
        if (v == null)
            throw new KafkaException("Value cannot be null.");

        return v;
    }

    /**
     * Parse a host and port out of a string
     */
    public Tuple2<String, Integer> parseHostPort(String hostport) {
        String[] splits = hostport.split(":");
        return Tuple2.make(splits[0], Integer.parseInt(splits[1]));
    }

    /**
     * Get the stack trace from an exception as a string
     */
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
     */
    public static Map<String, String> parseCsvMap(String str) {
        Map<String, String> map = new HashMap<String, String>();
        if ("".equals(str)) return map;


        String[] split = str.split("\\s*,\\s*");
        for (String kvStr : split) {
            String[] kv = kvStr.split("\\s*:\\s*");
            map.put(kv[0], kv[1]);
        }

        return map;
    }

    /**
     * Parse a comma separated string into a sequence of strings.
     * Whitespace surrounding the comma will be removed.
     */
    public static List<String> parseCsvList(String csvList) {
        List<String> list = new ArrayList<String>();
        if (csvList == null || csvList.isEmpty()) return list;

        String[] split = csvList.split("\\s*,\\s*");
        for (String v : split) {
            if (!v.equals("")) list.add(v);
        }

        return list;
    }

    /**
     * Create an instance of the class with the given class name
     */
    public static <T> T createObject(String className, Object... args) {
        try {
            Class<T> klass = (Class<T>) Class.forName(className);
            Class<?>[] argClasses = new Class<?>[args.length];
            for (int i = 0, ii = args.length; i < ii; ++i) {
                Object arg = args[i];
                argClasses[i] = arg.getClass();
            }

            Constructor<T> constructor = klass.getConstructor(argClasses);
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new KafkaException(e, "create instance of %s error", className);
        }

    }

    /**
     * Is the given string null or empty ("")?
     */
    public static boolean nullOrEmpty(String s) {
        return s == null || s.equals("");
    }

    /**
     * Create a circular (looping) iterator over a collection.
     *
     * @param coll An iterable over the underlying collection.
     * @return A circular iterator over the collection.
     */
    public static <T> Iterable<T> circularIterator(Iterable<T> coll) {
        return Iterables.cycle(coll);
    }

    /**
     * Attempt to read a file as a string
     */
    public static String readFileAsString(String path) {
        return readFileAsString(path, Charset.defaultCharset());
    }

    public static String readFileAsString(String path, Charset charset) {
        FileInputStream stream = null;

        try {
            stream = new FileInputStream(new File(path));
            FileChannel fc = stream.getChannel();
            MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            return charset.decode(bb).toString();
        } catch (IOException ex) {
            throw new KafkaException(ex, "readFileAsString %s error", path);
        } finally {
            closeQuietly(stream);
        }
    }

    /**
     * Get the absolute value of the given number. If the number is Int.MinValue return 0.
     * This is different from java.lang.Math.abs or scala.math.abs in that they return Int.MinValue (!).
     */
    public static int abs(int n) {
        return n & 0x7fffffff;
    }

    /**
     * Replace the given string suffix with the new suffix. If the string doesn't end with the given suffix throw an exception.
     */
    public static String replaceSuffix(String s, String oldSuffix, String newSuffix) {
        if (!s.endsWith(oldSuffix))
            throw new IllegalArgumentException(String.format(
                    "Expected string to end with '%s' but string is '%s'", oldSuffix, s));

        return s.substring(0, s.length() - oldSuffix.length()) + newSuffix;
    }

    /**
     * Create a file with the given path
     *
     * @param path The path to create
     * @return The created file
     * @throw KafkaStorageException If the file create fails
     */
    public static File createFile(String path) {
        File f = new File(path);
        boolean created = false;
        try {
            created = f.createNewFile();
        } catch (IOException e) {
            throw new KafkaStorageException(e, "Failed to create file %s.", path);
        }
        if (!created)
            throw new KafkaStorageException("Failed to create file %s.", path);

        return f;
    }

    /**
     * Turn a properties map into a string
     */
    public static String asString(Properties props) {
        StringWriter writer = new StringWriter();
        try {
            props.store(writer, "");
        } catch (IOException e) {
            throw new KafkaException(e, "asString error");
        }
        return writer.toString();
    }

    /**
     * Read some properties with the given default values
     */
    public static Properties readProps(String s, Properties defaults) {
        StringReader reader = new StringReader(s);
        Properties props = new Properties(defaults);
        try {
            props.load(reader);
        } catch (IOException e) {
            throw new KafkaException(e, "readProps error");
        }

        return props;
    }

    /**
     * Read a big-endian integer from a byte array
     */
    public static int readInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) |
                ((bytes[offset + 1] & 0xFF) << 16) |
                ((bytes[offset + 2] & 0xFF) << 8) |
                (bytes[offset + 3] & 0xFF);
    }

    /**
     * Execute the given function inside the lock
     */
    public static <T> T inLock(Lock lock, Function<T> fun) {
        lock.lock();
        try {
            return fun.apply();
        } finally {
            lock.unlock();
        }
    }
}
