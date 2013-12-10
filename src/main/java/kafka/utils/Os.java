package kafka.utils;

public interface Os {
    String name = System.getProperty("os.name").toLowerCase();
    boolean isWindows = name.startsWith("windows");
}
