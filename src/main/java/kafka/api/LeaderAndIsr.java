package kafka.api;

import com.google.common.collect.ImmutableMap;
import kafka.utils.Json;

import java.util.List;

public class LeaderAndIsr {
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;

    public LeaderAndIsr(int leader, int leaderEpoch, List<Integer> isr, int zkVersion) {
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
    }

    public LeaderAndIsr(int leader, List<Integer> isr) {
        this(leader, LeaderAndIsrs.initialLeaderEpoch, isr, LeaderAndIsrs.initialZKVersion);
    }

    @Override
    public String toString() {
        return Json.encode(ImmutableMap.of("leader", leader, "leader_epoch", leaderEpoch, "isr", isr));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderAndIsr that = (LeaderAndIsr) o;

        if (leader != that.leader) return false;
        if (leaderEpoch != that.leaderEpoch) return false;
        if (zkVersion != that.zkVersion) return false;
        if (isr != null ? !isr.equals(that.isr) : that.isr != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = leader;
        result = 31 * result + leaderEpoch;
        result = 31 * result + (isr != null ? isr.hashCode() : 0);
        result = 31 * result + zkVersion;
        return result;
    }
}
