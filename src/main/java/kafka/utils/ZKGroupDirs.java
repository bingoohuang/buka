package kafka.utils;

public class ZKGroupDirs {
    public final String group;

    public ZKGroupDirs(String group) {
        this.group = group;
    }

    public String consumerDir() {
        return ZkUtils.ConsumersPath;
    }

    public String consumerGroupDir() {
        return consumerDir() + "/" + group;
    }

    public String consumerRegistryDir() {
        return consumerGroupDir() + "/ids";
    }
}
