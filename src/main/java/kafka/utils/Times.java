package kafka.utils;

/**
 * Some common constants
 */
public interface Times {
    int NsPerUs = 1000;
    int UsPerMs = 1000;
    int MsPerSec = 1000;
    int NsPerMs = NsPerUs * UsPerMs;
    int NsPerSec = NsPerMs * MsPerSec;
    int UsPerSec = UsPerMs * MsPerSec;
    int SecsPerMin = 60;
    int MinsPerHour = 60;
    int HoursPerDay = 24;
    int SecsPerHour = SecsPerMin * MinsPerHour;
    int SecsPerDay = SecsPerHour * HoursPerDay;
    int MinsPerDay = MinsPerHour * HoursPerDay;
}
