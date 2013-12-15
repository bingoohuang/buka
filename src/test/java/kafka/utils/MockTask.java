package kafka.utils;

public class MockTask implements Comparable {
    String name;
    Runnable fun;
    long nextExecution;
    long period;

    public MockTask(String name, Runnable fun, long nextExecution, long period) {
        this.name = name;
        this.fun = fun;
        this.nextExecution = nextExecution;
        this.period = period;
    }

    public boolean periodic() {
        return period >= 0;
    }


    @Override
    public int compareTo(Object o) {
        MockTask t = (MockTask) o;
        if (t.nextExecution == nextExecution)
            return 0;
        else if (t.nextExecution < nextExecution)
            return -1;
        else
            return 1;
    }
}
