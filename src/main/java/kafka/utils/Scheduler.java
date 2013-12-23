package kafka.utils;

import java.util.concurrent.TimeUnit;

/**
 * A scheduler for running jobs
 *
 * This interface controls a job scheduler that allows scheduling either repeating background jobs
 * that execute periodically or delayed one-time actions that are scheduled in the future.
 */
public abstract class Scheduler {

    /**
     * Initialize this scheduler so it is ready to accept scheduling of tasks
     */
    public abstract void startup();

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
     * This includes tasks scheduled with a delayed execution.
     */
    public abstract void shutdown();

    public void schedule(String name, Runnable fun, long delay/* = 0*/) {
        schedule(name, fun, delay, -1, TimeUnit.MILLISECONDS);
    }

    /**
     * Schedule a task
     * @param name The name of this task
     * @param delay The amount of time to wait before the first execution
     * @param period The period with which to execute the task. If < 0 the task will execute only once.
     * @param unit The unit for the preceding times.
     */
    public abstract void schedule(String name, Runnable fun, long delay/* = 0*/, long period/* = -1*/, TimeUnit unit/*  = TimeUnit.MILLISECONDS*/);
}
