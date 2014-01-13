package kafka.metrics;

/**
 * Base trait for reporter MBeans. If a client wants to expose these JMX
 * operations on a custom reporter (that implements
 * [[kafka.metrics.KafkaMetricsReporter]]), the custom reporter needs to
 * additionally implement an MBean trait that extends this trait so that the
 * registered MBean is compliant with the standard MBean convention.
 */
public interface KafkaMetricsReporterMBean {
    void startReporter(long pollingPeriodInSeconds);
    void stopReporter();

    /**
     *
     * @return The name with which the MBean will be registered.
     */
    String getMBeanName();
}
