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
            return 1;
        else
            return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MockTask mockTask = (MockTask) o;

        if (nextExecution != mockTask.nextExecution) return false;
        if (period != mockTask.period) return false;
        if (fun != null ? !fun.equals(mockTask.fun) : mockTask.fun != null) return false;
        if (name != null ? !name.equals(mockTask.name) : mockTask.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (fun != null ? fun.hashCode() : 0);
        result = 31 * result + (int) (nextExecution ^ (nextExecution >>> 32));
        result = 31 * result + (int) (period ^ (period >>> 32));
        return result;
    }
}
