package kafka.network;

/**
 * A handler mapping finds the right Handler function for a given request
 */
public interface HandlerMapping {
    Handler apply(short code, Receive receive);
}
