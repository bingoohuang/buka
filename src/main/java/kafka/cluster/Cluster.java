package kafka.cluster;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * The set of active brokers in the cluster
 */
public class Cluster {
    private Map<Integer, Broker> brokers = Maps.newHashMap();

    public Cluster() {
    }

    public Cluster(Iterable<Broker> brokerList) {
        for (Broker broker : brokerList)
            brokers.put(broker.id, broker);
    }

    public Broker getBroker(int id) {
        return brokers.get(id);
    }

    public void add(Broker broker) {
        brokers.put(broker.id, broker);
    }

    public Broker remove(int id) {
        return brokers.remove(id);
    }

    public int size() {
        return brokers.size();
    }

    @Override
    public String toString() {
        return "Cluster(" + brokers.values() + ")";
    }
}
