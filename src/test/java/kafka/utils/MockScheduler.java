package kafka.utils;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 * <p/>
 * Example usage
 * <code>
 * val time = new MockTime
 * time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 * time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 * <p/>
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
public class MockScheduler extends Scheduler {
    public Time time;

    public MockScheduler(Time time) {
        this.time = time;
    }

    /* a priority queue of tasks ordered by next execution time */
    PriorityQueue<MockTask> tasks = new PriorityQueue<MockTask>();

    public void startup() {
    }

    public void shutdown() {
        synchronized (this) {
            tasks.clear();
        }
    }

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    public void tick() {
        synchronized (this) {
            long now = time.milliseconds();
            while (!tasks.isEmpty() && tasks.peek().nextExecution <= now) {
        /* pop and execute the task with the lowest next execution time */
                MockTask curr = tasks.poll();
                curr.fun.run();
        /* if the task is periodic, reschedule it and re-enqueue */
                if (curr.periodic()) {
                    curr.nextExecution += curr.period;
                    this.tasks.add(curr);
                }
            }
        }
    }

    @Override
    public void schedule(String name, Runnable fun, long delay, long period, TimeUnit unit) {
        synchronized (this) {
            tasks.add(new MockTask(name, fun, time.milliseconds() + delay, period));
            tick();
        }
    }
}
