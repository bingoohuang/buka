package kafka.log;

import kafka.utils.SystemTime;
import kafka.utils.Time;

/**
 * A simple struct for collecting stats about log cleaning
 */
public class CleanerStats {
    public Time time;

    public CleanerStats() {
        this(SystemTime.instance);
    }

    public CleanerStats(Time time) {
        this.time = time;
        clear();
    }

    public long startTime, mapCompleteTime, endTime, bytesRead, bytesWritten, mapBytesRead, mapMessagesRead, messagesRead, messagesWritten;


    public void readMessage(int size) {
        messagesRead += 1;
        bytesRead += size;
    }

    public void recopyMessage(int size) {
        messagesWritten += 1;
        bytesWritten += size;
    }

    public void indexMessage(int size) {
        mapMessagesRead += 1;
        mapBytesRead += size;
    }

    public void indexDone() {
        mapCompleteTime = time.milliseconds();
    }

    public void allDone() {
        endTime = time.milliseconds();
    }

    public double elapsedSecs() {
        return (endTime - startTime) / 1000.0;
    }

    public double elapsedIndexSecs() {
        return (mapCompleteTime - startTime) / 1000.0;
    }

    public void clear() {
        startTime = time.milliseconds();
        mapCompleteTime = -1L;
        endTime = -1L;
        bytesRead = 0L;
        bytesWritten = 0L;
        mapBytesRead = 0L;
        mapMessagesRead = 0L;
        messagesRead = 0L;
        messagesWritten = 0L;
    }
}
