package kafka.admin;

import com.alibaba.fastjson.JSONObject;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import kafka.common.BrokerNotAvailableException;
import kafka.common.TopicAndPartition;
import kafka.controller.KafkaControllers;
import kafka.utils.CommandLineUtils;
import kafka.utils.Json;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.Set;

public class ShutdownBroker {
    static class ShutdownParams {
        public String zkConnect;
        public Integer brokerId;

        ShutdownParams(String zkConnect, Integer brokerId) {
            this.zkConnect = zkConnect;
            this.brokerId = brokerId;
        }
    }

    static Logger logger = LoggerFactory.getLogger(ShutdownBroker.class);

    private static boolean invokeShutdown(ShutdownParams params) {
        ZkClient zkClient = null;
        try {
            zkClient = new ZkClient(params.zkConnect, 30000, 30000, ZKStringSerializer.instance);
            int controllerBrokerId = ZkUtils.getController(zkClient);
            String controllerInfo = ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + controllerBrokerId)._1;
            if (controllerInfo == null) {
                throw new BrokerNotAvailableException("Broker id %d does not exist", controllerBrokerId);
            }

            String controllerHost = null;
            int controllerJmxPort = -1;

            JSONObject brokerInfo = Json.parseFull(controllerInfo);
            if (brokerInfo == null) {
                throw new BrokerNotAvailableException("Broker id %d does not exist", controllerBrokerId);
            }

            controllerHost = brokerInfo.getString("host");
            controllerJmxPort = brokerInfo.getIntValue("jmx_port");


            JMXServiceURL jmxUrl = new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", controllerHost, controllerJmxPort));
            logger.info("Connecting to jmx url {}", jmxUrl);
            JMXConnector jmxc = JMXConnectorFactory.connect(jmxUrl, null);
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            Set<TopicAndPartition> leaderPartitionsRemaining = (Set<TopicAndPartition>) mbsc.invoke(new ObjectName(KafkaControllers.MBeanName),
                    "shutdownBroker",
                    new Object[]{params.brokerId},
                    new String[]{Integer.class.getName()});
            boolean shutdownComplete = (leaderPartitionsRemaining.size() == 0);
            logger.info("Shutdown status: " +
                    ((shutdownComplete) ? "complete" : "incomplete (broker still leads {} partitions)"), leaderPartitionsRemaining);
            return shutdownComplete;

        } catch (Throwable t) {
            logger.error("Operation failed due to controller failure", t);
            return false;
        } finally {
            if (zkClient != null)
                zkClient.close();
        }
    }

    public static void main(String[] args) {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> brokerOpt = parser.accepts("broker", "REQUIRED: The broker to shutdown.")
                .withRequiredArg()
                .describedAs("Broker Id")
                .ofType(Integer.class);
        OptionSpec<String> zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                "Multiple URLS can be given to allow fail-over.")
                .withRequiredArg()
                .describedAs("urls")
                .ofType(String.class);
        OptionSpec<Integer> numRetriesOpt = parser.accepts("num.retries", "Number of attempts to retry if shutdown does not complete.")
                .withRequiredArg()
                .describedAs("number of retries")
                .ofType(Integer.class)
                .defaultsTo(0);
        OptionSpec<Integer> retryIntervalOpt = parser.accepts("retry.interval.ms", "Retry interval if retries requested.")
                .withRequiredArg()
                .describedAs("retry interval in ms (> 1000)")
                .ofType(Integer.class)
                .defaultsTo(1000);

        OptionSet options = parser.parse(args);
        CommandLineUtils.checkRequiredArgs(parser, options, brokerOpt, zkConnectOpt);

        int retryIntervalMs = Math.max(options.valueOf(retryIntervalOpt).intValue(), 1000);
        int numRetries = options.valueOf(numRetriesOpt).intValue();

        ShutdownParams shutdownParams = new ShutdownParams(options.valueOf(zkConnectOpt), options.valueOf(brokerOpt));

        if (!invokeShutdown(shutdownParams)) {
            for (int attempt = 1; attempt <= numRetries; ++attempt) {
                logger.info("Retry {}", attempt);
                try {
                    Thread.sleep(retryIntervalMs);
                } catch (InterruptedException e) {
                    // ignore
                }
                if (invokeShutdown(shutdownParams)) break;
            }
        }
    }
}
