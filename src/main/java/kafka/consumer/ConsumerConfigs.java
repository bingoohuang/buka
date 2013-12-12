package kafka.consumer;

import kafka.api.OffsetRequestReader;
import kafka.common.Config;
import kafka.common.InvalidConfigException;

public class ConsumerConfigs {
    public final static int RefreshMetadataBackoffMs = 200;
    public final static int SocketTimeout = 30 * 1000;
    public final static int SocketBufferSize = 64 * 1024;
    public final static int FetchSize = 1024 * 1024;
    public final static int MaxFetchSize = 10 * FetchSize;
    public final static int DefaultFetcherBackoffMs = 1000;
    public final static boolean AutoCommit = true;
    public final static int AutoCommitInterval = 60 * 1000;
    public final static int MaxQueuedChunks = 2;
    public final static int MaxRebalanceRetries = 4;
    public final static String AutoOffsetReset = OffsetRequestReader.LargestTimeString;
    public final static int ConsumerTimeoutMs = -1;
    public final static int MinFetchBytes = 1;
    public final static int MaxFetchWaitMs = 100;
    public final static String MirrorTopicsWhitelist = "";
    public final static String MirrorTopicsBlacklist = "";
    public final static int MirrorConsumerNumThreads = 1;

    public final static String MirrorTopicsWhitelistProp = "mirror.topics.whitelist";
    public final static String MirrorTopicsBlacklistProp = "mirror.topics.blacklist";
    public final static String MirrorConsumerNumThreadsProp = "mirror.consumer.numthreads";
    public final static String DefaultClientId = "";

    public static void validate(ConsumerConfig config) {
        validateClientId(config.clientId);
        validateGroupId(config.groupId);
        validateAutoOffsetReset(config.autoOffsetReset);
    }

    public static void validateClientId(String clientId) {
        Config.validateChars("client.id", clientId);
    }

    public static void validateGroupId(String groupId) {
        Config.validateChars("group.id", groupId);
    }

    public static void validateAutoOffsetReset(String autoOffsetReset) {
        if (autoOffsetReset == OffsetRequestReader.SmallestTimeString) {
        } else if (autoOffsetReset == OffsetRequestReader.LargestTimeString) {
        } else {
            throw new InvalidConfigException("Wrong value " + autoOffsetReset + " of auto.offset.reset in ConsumerConfig; " +
                    "Valid values are " + OffsetRequestReader.SmallestTimeString + " and " + OffsetRequestReader.LargestTimeString);
        }
    }
}
