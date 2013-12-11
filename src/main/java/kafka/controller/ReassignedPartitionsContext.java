package kafka.controller;

import com.google.common.collect.Lists;

import java.util.Collection;

public class ReassignedPartitionsContext {
    public final Collection<Integer> newReplicas;
    public ReassignedPartitionsIsrChangeListener isrChangeListener;

    public ReassignedPartitionsContext() {
        this(Lists.<Integer>newArrayList(), null);
    }

    public ReassignedPartitionsContext(Collection<Integer> newReplicas,
                                       ReassignedPartitionsIsrChangeListener isrChangeListener) {
        this.newReplicas = newReplicas;
        this.isrChangeListener = isrChangeListener;
    }

    @Override
    public String toString() {
        return "ReassignedPartitionsContext{" +
                "newReplicas=" + newReplicas +
                ", isrChangeListener=" + isrChangeListener +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReassignedPartitionsContext that = (ReassignedPartitionsContext) o;

        if (isrChangeListener != null ? !isrChangeListener.equals(that.isrChangeListener) : that.isrChangeListener != null)
            return false;
        if (newReplicas != null ? !newReplicas.equals(that.newReplicas) : that.newReplicas != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = newReplicas != null ? newReplicas.hashCode() : 0;
        result = 31 * result + (isrChangeListener != null ? isrChangeListener.hashCode() : 0);
        return result;
    }
}
