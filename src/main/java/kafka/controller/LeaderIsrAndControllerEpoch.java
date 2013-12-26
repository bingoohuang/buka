package kafka.controller;

import kafka.api.LeaderAndIsr;

public class LeaderIsrAndControllerEpoch {
    public LeaderAndIsr leaderAndIsr;
    public int controllerEpoch;

    public LeaderIsrAndControllerEpoch(LeaderAndIsr leaderAndIsr, int controllerEpoch) {
        this.leaderAndIsr = leaderAndIsr;
        this.controllerEpoch = controllerEpoch;
    }

    @Override
    public String toString() {
        StringBuilder leaderAndIsrInfo = new StringBuilder();
        leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader);
        leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr);
        leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch);
        leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")");
        return leaderAndIsrInfo.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LeaderIsrAndControllerEpoch that = (LeaderIsrAndControllerEpoch) o;

        if (controllerEpoch != that.controllerEpoch) return false;
        if (leaderAndIsr != null ? !leaderAndIsr.equals(that.leaderAndIsr) : that.leaderAndIsr != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = leaderAndIsr != null ? leaderAndIsr.hashCode() : 0;
        result = 31 * result + controllerEpoch;
        return result;
    }
}
