package kafka.admin;

import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.utils.Utils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;

public class DeleteTopicCommand {
    public static void main(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        OptionSpec<String> topicOpt = parser.accepts("topic", "REQUIRED: The topic to be deleted.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        OptionSpec<String> zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                "Multiple URLS can be given to allow fail-over.")
                .withRequiredArg()
                .describedAs("urls")
                .ofType(String.class);

        OptionSet options = parser.parse(args);

        for (OptionSpec<String> arg : Lists.newArrayList(topicOpt, zkConnectOpt)) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument \"" + arg + "\"");
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }

        String topic = options.valueOf(topicOpt);
        String zkConnect = options.valueOf(zkConnectOpt);
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer.instance);
            zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
            System.out.println("deletion succeeded!");
        } catch (Throwable e) {
            System.out.println("delection failed because of " + e.getMessage());
            System.out.println(Utils.stackTrace(e));
        } finally {
            if (zkClient != null)
                zkClient.close();
        }
    }
}
