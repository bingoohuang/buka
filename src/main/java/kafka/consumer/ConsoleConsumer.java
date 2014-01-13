package kafka.consumer;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.common.KafkaException;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.metrics.KafkaMetricsReporter;
import kafka.serializer.DefaultDecoder;
import kafka.utils.CommandLineUtils;
import kafka.utils.Function1;
import kafka.utils.Utils;
import kafka.utils.VerifiableProperties;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * Consumer that dumps messages out to standard out.
 */
public class ConsoleConsumer {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        OptionParser parser = new OptionParser();
        OptionSpec<String> topicIdOpt = parser.accepts("topic", "The topic id to consume on.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        OptionSpec<String> whitelistOpt = parser.accepts("whitelist", "Whitelist of topics to include for consumption.")
                .withRequiredArg()
                .describedAs("whitelist")
                .ofType(String.class);
        OptionSpec<String> blacklistOpt = parser.accepts("blacklist", "Blacklist of topics to exclude from consumption.")
                .withRequiredArg()
                .describedAs("blacklist")
                .ofType(String.class);
        final OptionSpec<String> zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                "Multiple URLS can be given to allow fail-over.")
                .withRequiredArg()
                .describedAs("urls")
                .ofType(String.class);
        final OptionSpec<String> groupIdOpt = parser.accepts("group", "The group id to consume on.")
                .withRequiredArg()
                .describedAs("gid")
                .defaultsTo("console-consumer-" + new Random().nextInt(100000))
                .ofType(String.class);
        OptionSpec<Integer> fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(1024 * 1024);
        OptionSpec<Integer> minFetchBytesOpt = parser.accepts("min-fetch-bytes", "The min number of bytes each fetch request waits for.")
                .withRequiredArg()
                .describedAs("bytes")
                .ofType(Integer.class)
                .defaultsTo(1);
        OptionSpec<Integer> maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(100);
        OptionSpec<Integer> socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                .withRequiredArg()
                .describedAs("size")
                .ofType(Integer.class)
                .defaultsTo(2 * 1024 * 1024);
        OptionSpec<Integer> socketTimeoutMsOpt = parser.accepts("socket-timeout-ms", "The socket timeout used for the connection to the broker")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(ConsumerConfigs.SocketTimeout);
        OptionSpec<Integer> refreshMetadataBackoffMsOpt = parser.accepts("refresh-leader-backoff-ms", "Backoff time before refreshing metadata")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(ConsumerConfigs.RefreshMetadataBackoffMs);
        OptionSpec<Integer> consumerTimeoutMsOpt = parser.accepts("consumer-timeout-ms", "consumer throws timeout exception after waiting this much " +
                "of time without incoming messages")
                .withRequiredArg()
                .describedAs("prop")
                .ofType(Integer.class)
                .defaultsTo(-1);
        OptionSpec<String> messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
                .withRequiredArg()
                .describedAs("class")
                .ofType(String.class)
                .defaultsTo(DefaultMessageFormatter.class.getName());
        OptionSpec<String> messageFormatterArgOpt = parser.accepts("property")
                .withRequiredArg()
                .describedAs("prop")
                .ofType(String.class);
        OptionSpec<?> resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
                "start with the earliest message present in the log rather than the latest message.");
        OptionSpec<Integer> autoCommitIntervalOpt = parser.accepts("autocommit.interval.ms", "The time interval at which to save the current offset in ms")
                .withRequiredArg()
                .describedAs("ms")
                .ofType(Integer.class)
                .defaultsTo(ConsumerConfigs.AutoCommitInterval);
        OptionSpec<Integer> maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
                .withRequiredArg()
                .describedAs("num_messages")
                .ofType(Integer.class);
        OptionSpec<?> skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
                "skip it instead of halt.");
        OptionSpec<?> csvMetricsReporterEnabledOpt = parser.accepts("csv-reporter-enabled", "If set, the CSV metrics reporter will be enabled");
        OptionSpec<String> metricsDirectoryOpt = parser.accepts("metrics-dir", "If csv-reporter-enable is set, and this parameter is" +
                "set, the csv metrics will be outputed here")
                .withRequiredArg()
                .describedAs("metrics dictory")
                .ofType(String.class);


        final OptionSet options = tryParse(parser, args);
        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt);
        List<OptionSpec<?>> topicOrFilterOpt = Utils.filter(Lists.<OptionSpec<?>>newArrayList(topicIdOpt, whitelistOpt, blacklistOpt), new Predicate<OptionSpec<?>>() {
            @Override
            public boolean apply(OptionSpec<?> _) {
                return options.has(_);
            }
        });
        if (topicOrFilterOpt.size() != 1) {
            logger.error("Exactly one of whitelist/blacklist/topic is required.");
            parser.printHelpOn(System.err);
            System.exit(1);
        }
        String topicArg = (String) options.valueOf(Utils.head(topicOrFilterOpt));
        TopicFilter filterSpec = options.has(blacklistOpt) ? new Blacklist(topicArg) : new Whitelist(topicArg);

        boolean csvMetricsReporterEnabled = options.has(csvMetricsReporterEnabledOpt);
        if (csvMetricsReporterEnabled) {
            Properties csvReporterProps = new Properties();
            csvReporterProps.put("kafka.metrics.polling.interval.secs", "5");
            csvReporterProps.put("kafka.metrics.reporters", "kafka.metrics.KafkaCSVMetricsReporter");
            if (options.has(metricsDirectoryOpt))
                csvReporterProps.put("kafka.csv.metrics.dir", options.valueOf(metricsDirectoryOpt));
            else
                csvReporterProps.put("kafka.csv.metrics.dir", "kafka_metrics");
            csvReporterProps.put("kafka.csv.metrics.reporter.enabled", "true");
            VerifiableProperties verifiableProps = new VerifiableProperties(csvReporterProps);
            KafkaMetricsReporter.startReporters(verifiableProps);
        }

        Properties props = new Properties();
        props.put("group.id", options.valueOf(groupIdOpt));
        props.put("socket.receive.buffer.bytes", options.valueOf(socketBufferSizeOpt).toString());
        props.put("socket.timeout.ms", options.valueOf(socketTimeoutMsOpt).toString());
        props.put("fetch.message.max.bytes", options.valueOf(fetchSizeOpt).toString());
        props.put("fetch.min.bytes", options.valueOf(minFetchBytesOpt).toString());
        props.put("fetch.wait.max.ms", options.valueOf(maxWaitMsOpt).toString());
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", options.valueOf(autoCommitIntervalOpt).toString());
        props.put("auto.offset.reset", (options.has(resetBeginningOpt)) ? "smallest" : "largest");
        props.put("zookeeper.connect", options.valueOf(zkConnectOpt));
        props.put("consumer.timeout.ms", options.valueOf(consumerTimeoutMsOpt).toString());
        props.put("refresh.leader.backoff.ms", options.valueOf(refreshMetadataBackoffMsOpt).toString());

        ConsumerConfig config = new ConsumerConfig(props);
        boolean skipMessageOnError = (options.has(skipMessageOnErrorOpt));

        Class<? extends MessageFormatter> messageFormatterClass = (Class<? extends MessageFormatter>)
                Class.forName(options.valueOf(messageFormatterOpt));
        Properties formatterArgs = MessageFormatter.tryParseFormatterArgs(options.valuesOf(messageFormatterArgOpt));

        int maxMessages = (options.has(maxMessagesOpt)) ? options.valueOf(maxMessagesOpt).intValue() : -1;

        final ConsumerConnector connector = Consumer.create(config);

        if (options.has(resetBeginningOpt))
            ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + options.valueOf(groupIdOpt));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                connector.shutdown();
                // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
                if (!options.has(groupIdOpt))
                    ZkUtils.maybeDeletePath(options.valueOf(zkConnectOpt), "/consumers/" + options.valueOf(groupIdOpt));
            }
        });

        long numMessages = 0L;
        MessageFormatter formatter = Utils.newInstance(messageFormatterClass);
        formatter.init(formatterArgs);
        try {
            KafkaStream<byte[], byte[]> stream = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder()).get(0);
            Iterable<MessageAndMetadata<byte[], byte[]>> iter = stream;
            if (maxMessages >= 0) {

                int numberToSkip = Iterables.size(stream) - maxMessages;
                if (numberToSkip > 0) {
                    iter = Iterables.skip(stream, numberToSkip);
                }
            }


            for (MessageAndMetadata<byte[], byte[]> messageAndTopic : iter) {
                try {
                    formatter.writeTo(messageAndTopic.key(), messageAndTopic.message(), System.out);
                    numMessages += 1;
                } catch (Throwable e) {
                    if (skipMessageOnError)
                        logger.error("Error processing message, skipping this message: ", e);
                    else
                        throw e;
                }
                if (System.out.checkError()) {
                    // This means no one is listening to our output stream any more, time to shutdown
                    System.err.println("Unable to write to standard out, closing consumer.");
                    System.err.println(String.format("Consumed %d messages", numMessages));
                    formatter.close();
                    connector.shutdown();
                    System.exit(1);
                }
            }
        } catch (Throwable e) {
            logger.error("Error processing message, stopping consumer: ", e);
        }
        System.err.println(String.format("Consumed %d messages", numMessages));
        System.out.flush();
        formatter.close();
        connector.shutdown();
    }


    static Logger logger = LoggerFactory.getLogger(ConsoleConsumer.class);

    public static OptionSet tryParse(OptionParser parser, String[] args) {
        try {
            return parser.parse(args);
        } catch (OptionException e) {
            Utils.croak(e.getMessage());
            return null;
        }

    }

    public static void tryCleanupZookeeper(String zkUrl, String groupId) {
        try {
            String dir = "/consumers/" + groupId;
            logger.info("Cleaning up temporary zookeeper data under " + dir + ".");
            ZkClient zk = new ZkClient(zkUrl, 30 * 1000, 30 * 1000, ZKStringSerializer.instance);
            zk.deleteRecursive(dir);
            zk.close();
        } catch (Throwable e) {
            // swallow
        }
    }

    static abstract class MessageFormatter {
        public static Properties tryParseFormatterArgs(Iterable<String> args) {
            List<String[]> splits = Utils.filter(Utils.mapList(args, new Function1<String, String[]>() {
                @Override
                public String[] apply(String _) {
                    return _.split("=");
                }
            }), new Predicate<String[]>() {
                @Override
                public boolean apply(String[] input) {
                    return input != null && input.length > 0;
                }
            });

            if (!Utils.forall(splits, new Predicate<String[]>() {
                @Override
                public boolean apply(String[] input) {
                    return input.length == 2;
                }
            })) {
                System.err.println("Invalid parser arguments: " + Iterables.toString(args));
                System.exit(1);
            }
            Properties props = new Properties();
            for (String[] a : splits)
                props.put(a[0], a[1]);

            return props;
        }

        public abstract void writeTo(byte[] key, byte[] value, PrintStream output);

        public void init(Properties props) {
        }

        public void close() {
        }
    }

    static class DefaultMessageFormatter extends MessageFormatter {
        boolean printKey = false;
        byte[] keySeparator = "\t".getBytes();
        byte[] lineSeparator = "\n".getBytes();

        @Override
        public void init(Properties props) {
            if (props.containsKey("print.key"))
                printKey = props.getProperty("print.key").trim().toLowerCase().equals("true");
            if (props.containsKey("key.separator"))
                keySeparator = props.getProperty("key.separator").getBytes();
            if (props.containsKey("line.separator"))
                lineSeparator = props.getProperty("line.separator").getBytes();
        }

        @Override
        public void writeTo(byte[] key, byte[] value, PrintStream output) {
            try {
                if (printKey) {
                    output.write((key == null) ? "null".getBytes() : key);
                    output.write(keySeparator);
                }
                output.write((value == null) ? "null".getBytes() : value);
                output.write(lineSeparator);
            } catch (IOException e) {
                throw new KafkaException(e);
            }
        }
    }

    static class NoOpMessageFormatter extends MessageFormatter {

        @Override
        public void writeTo(byte[] key, byte[] value, PrintStream output) {

        }
    }

    static class ChecksumMessageFormatter extends MessageFormatter {
        private String topicStr;

        @Override
        public void init(Properties props) {
            topicStr = props.getProperty("topic");
            if (topicStr != null)
                topicStr = topicStr + ":";
            else
                topicStr = "";
        }

        @Override
        public void writeTo(byte[] key, byte[] value, PrintStream output) {
            long chksum = new Message(value, key).checksum();
            output.println(topicStr + "checksum:" + chksum);
        }
    }
}
