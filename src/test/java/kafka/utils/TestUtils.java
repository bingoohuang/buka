package kafka.utils;

import com.google.common.base.Joiner;
import junit.framework.AssertionFailedError;
import kafka.common.KafkaException;
import kafka.message.*;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Utility functions to help with testing
 */
public class TestUtils {
    public static String IoTmpDir = System.getProperty("java.io.tmpdir");

    public static String Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static String Digits = "0123456789";
    public static String LettersAndDigits = Letters + Digits;

    /* A consistent random number generator to make tests repeatable */
    public static Random seededRandom = new Random(192348092834L);
    public static Random random = new Random();


    /**
     * Choose a number of random available ports
     */
    public static List<Integer> choosePorts(int count) {
        List<ServerSocket> socketList = Utils.flatList(0, count, new Function1<Integer, ServerSocket>() {
            @Override
            public ServerSocket apply(Integer arg) {
                try {
                    return new ServerSocket(0);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return Utils.mapList(socketList, new Function1<ServerSocket, Integer>() {
            @Override
            public Integer apply(ServerSocket _) {
                int localPort = _.getLocalPort();
                Utils.closeQuietly(_);
                return localPort;
            }
        });
    }

    /**
     * Choose an available port
     */
    public static int choosePort() {
        return choosePorts(1).get(0);
    }

    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        File f = new File(IoTmpDir, "kafka-" + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    /**
     * Create a temporary file
     */
    public static File tempFile() {
        File f;
        try {
            f = File.createTempFile("kafka", ".tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        f.deleteOnExit();
        return f;
    }

    /**
     * Create a temporary file and return an open file channel for this file
     */
    public static FileChannel tempChannel() {
        try {
            return new RandomAccessFile(tempFile(), "rw").getChannel();
        } catch (FileNotFoundException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Create a kafka server instance with appropriate test settings
     * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
     *
     * @param config The configuration of the server
     */
    public KafkaServer createServer(KafkaConfig config) {
        return createServer(config, SystemTime.instance);
    }

    public KafkaServer createServer(KafkaConfig config, Time time) {
        KafkaServer server = new KafkaServer(config, time);
        server.startup();
        return server;
    }

    /**
     * Create a test config for the given node id
     */
    public static List<Properties> createBrokerConfigs(int numConfigs) {
        return Utils.mapList(Utils.zipWithIndex(choosePorts(numConfigs)), new Function1<Tuple2<Integer, Integer>, Properties>() {
            @Override
            public Properties apply(Tuple2<Integer, Integer> arg) {
                return createBrokerConfig(arg._2, arg._1);
            }
        });
    }

    public static String getBrokerListStrFromConfigs(List<KafkaConfig> configs) {
        return Joiner.on(',').join(Utils.mapList(configs, new Function1<KafkaConfig, String>() {
            @Override
            public String apply(KafkaConfig c) {
                return c.hostName + ":" + c.port;
            }
        }));
    }

    /**
     * Create a test config for the given node id
     */
    public static Properties createBrokerConfig(int nodeId) {
        return createBrokerConfig(nodeId, choosePort());
    }

    public static Properties createBrokerConfig(int nodeId, int port) {
        Properties props = new Properties();
        props.put("broker.id", nodeId + "");
        props.put("host.name", "localhost");
        props.put("port", port + "");
        props.put("log.dir", TestUtils.tempDir().getAbsolutePath());
        props.put("zookeeper.connect", TestZKUtils.zookeeperConnect);
        props.put("replica.socket.timeout.ms", "1500");
        return props;
    }

    /**
     * Create a test config for a consumer
     */
    public static Properties createConsumerProperties(String zkConnect, String groupId, String consumerId) {
        return createConsumerProperties(zkConnect, groupId, consumerId, -1L);
    }

    public static Properties createConsumerProperties(String zkConnect, String groupId, String consumerId, Long consumerTimeout) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        props.put("consumer.id", consumerId);
        props.put("consumer.timeout.ms", consumerTimeout + "");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.max.retries", "4");
        props.put("auto.offset.reset", "smallest");

        return props;
    }

    /**
     * Wrap the message in a message set
     *
     * @param payload The bytes of the message
     */
    public static ByteBufferMessageSet singleMessageSet(byte[] payload) {
        return singleMessageSet(payload, NoCompressionCodec.instance, null);
    }

    public static ByteBufferMessageSet singleMessageSet(byte[] payload, byte[] key) {
        return singleMessageSet(payload, NoCompressionCodec.instance, key);
    }
    public static ByteBufferMessageSet singleMessageSet(byte[] payload, CompressionCodec codec, byte[] key) {
        return new ByteBufferMessageSet(codec, new Message(payload, key));
    }

    /**
     * Generate an array of random bytes
     *
     * @param numBytes The size of the array
     */
    public static byte[] randomBytes(int numBytes) {
        byte[] bytes = new byte[numBytes];
        seededRandom.nextBytes(bytes);
        return bytes;
    }


    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(int len) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; ++i)
            b.append(LettersAndDigits.charAt(seededRandom.nextInt(LettersAndDigits.length())));

        return b.toString();
    }

    /**
     * Check that the buffer content from buffer.position() to buffer.limit() is equal
     */
    public static void checkEquals(ByteBuffer b1, ByteBuffer b2) {
        assertEquals("Buffers should have equal length", b1.limit() - b1.position(), b2.limit() - b2.position());
        for (int i = 0, ii = b1.limit() - b1.position(); i < ii; ++i)
            assertEquals("byte " + i + " byte not equal.", b1.get(b1.position() + i), b2.get(b1.position() + i));
    }

    /**
     * Throw an exception if the two iterators are of differing lengths or contain
     * different messages on their Nth element
     */
    public static <T> void checkEquals(Iterator<T> expected, Iterator<T> actual) {
        int length = 0;
        while (expected.hasNext() && actual.hasNext()) {
            length += 1;
            assertEquals(expected.next(), actual.next());
        }

        // check if the expected iterator is longer
        if (expected.hasNext()) {
            int length1 = length;
            while (expected.hasNext()) {
                expected.next();
                length1 += 1;
            }
            assertFalse("Iterators have uneven length-- first has more: " + length1 + " > " + length, true);
        }

        // check if the actual iterator was longer
        if (actual.hasNext()) {
            int length2 = length;
            while (actual.hasNext()) {
                actual.next();
                length2 += 1;
            }
            assertFalse("Iterators have uneven length-- second has more: " + length2 + " > " + length, true);
        }
    }

    /**
     * Throw an exception if an iterable has different length than expected
     */
    public static <T> void checkLength(Iterator<T> s1, int expectedLength) {
        int n = 0;
        while (s1.hasNext()) {
            n += 1;
            s1.next();
        }
        assertEquals(expectedLength, n);
    }


    public static Iterator<Message> getMessageIterator(final Iterator<MessageAndOffset> iter) {
        return new IteratorTemplate<Message>() {
            @Override
            public Message makeNext() {
                if (iter.hasNext())
                    return iter.next().message;
                else
                    return allDone();
            }
        };
    }

    public static <T> Iterator<T> stackedIterator(final List<Iterator<T>> s) {
        return new Iterator<T>() {
            Iterator<T> cur = null;
            Iterator<Iterator<T>> topIterator = s.iterator();

            public boolean hasNext() {
                while (true) {
                    if (cur == null) {
                        if (topIterator.hasNext())
                            cur = topIterator.next();
                        else
                            return false;
                    }
                    if (cur.hasNext())
                        return true;
                    cur = null;
                }
                // should never reach her
                // throw new RuntimeException("should not reach here");
            }

            public T next() {
                return cur.next();
            }

            public void remove() {

            }
        };
    }

    /**
     * Create a hexidecimal string for the given bytes
     */
    public static String hexString(byte[] bytes) {
        return hexString(ByteBuffer.wrap(bytes));
    }

    /**
     * Create a hexidecimal string for the given bytes
     */
    public static String hexString(ByteBuffer buffer) {
        StringBuilder builder = new StringBuilder("0x");
        for (int i = 0; i < buffer.limit(); ++i)
            builder.append(String.format("%x", Integer.valueOf(buffer.get(buffer.position() + i))));
        return builder.toString();
    }

    static  Logger logger = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Execute the given block. If it throws an assert error, retry. Repeat
     * until no error is thrown or the time limit ellapses
     */
    public static void retry(long maxWaitMs, Runnable block) {
        long wait = 1L;
        long startTime = System.currentTimeMillis();
        while(true) {
            try {
                block.run();
                return;
            } catch (AssertionFailedError e){
                    long ellapsed = System.currentTimeMillis() - startTime;
                    if(ellapsed > maxWaitMs) {
                        throw e;
                    } else {
                        logger.info("Attempt failed, sleeping for " + wait + ", and then retrying.");
                        try {
                            Thread.sleep(wait);
                        } catch (InterruptedException e1) {
                            throw new KafkaException(e1);
                        }
                        wait += Math.min(wait, 1000);
                    }
            }
        }
    }

    public static void writeNonsenseToFile(File fileName, long position, int size) {
        try {
            RandomAccessFile file = new RandomAccessFile(fileName, "rw");
            file.seek(position);
            for (int i = 0; i < size; ++i)
                file.writeByte(random.nextInt(255));
            file.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }


    public static void appendNonsenseToFile(File fileName, int size) {
        try {
            FileOutputStream file = new FileOutputStream(fileName, true);
            for (int i = 0; i < size; ++i)
            file.write(random.nextInt(255));
            file.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }


    /**
     * Wait until the given condition is true or the given wait time ellapses
     */
    public static boolean waitUntilTrue(Function0<Boolean> condition, long waitTime) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (condition.apply())
                return true;
            if (System.currentTimeMillis() > startTime + waitTime)
                return false;
            try {
                Thread.sleep(Math.min(waitTime, 100L));
            } catch (InterruptedException e) {
                // ignore
            }
        }
        // should never hit here
        // throw new RuntimeException("unexpected error");
    }
}
