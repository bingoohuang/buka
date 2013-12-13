package kafka.log;

import java.io.File;
import java.text.NumberFormat;

/**
 * Helper functions for logs
 */
public class Logs {
    /**
     * a log file
     */
    public static final String LogFileSuffix = ".log";

    /**
     * an index file
     */
    public static final String IndexFileSuffix = ".index";

    /**
     * a file that is scheduled to be deleted
     */
    public static final String DeletedFileSuffix = ".deleted";

    /**
     * A temporary file that is being used for log cleaning
     */
    public static final String CleanedFileSuffix = ".cleaned";

    /**
     * A temporary file used when swapping files into the log
     */
    public static final String SwapFileSuffix = ".swap";

    /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
     * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
    /**
     * TODO: Get rid of CleanShutdownFile in 0.8.2
     */
    public static final String CleanShutdownFile = ".kafka_cleanshutdown";

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     */
    public static String filenamePrefixFromOffset(Long offset) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * Construct a log file name in the given dir with the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File logFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix);
    }

    /**
     * Construct an index file name in the given dir using the given base offset
     *
     * @param dir    The directory in which the log will reside
     * @param offset The base offset of the log file
     */
    public static File indexFilename(File dir, Long offset) {
        return new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix);
    }
}
