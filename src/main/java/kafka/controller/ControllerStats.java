package kafka.controller;

import com.yammer.metrics.core.Meter;
import kafka.metrics.KafkaMetricsGroup;
import kafka.metrics.KafkaTimer;

import java.util.concurrent.TimeUnit;

public class ControllerStats extends KafkaMetricsGroup {
    public static ControllerStats instance = new ControllerStats();

    public Meter uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS);
    public KafkaTimer leaderElectionTimer = new KafkaTimer(newTimer("LeaderElectionRateAndTimeMs", TimeUnit.MILLISECONDS, TimeUnit.SECONDS));
}
