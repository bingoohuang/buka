package kafka.network;


public interface Handler {
    /**
     * A request handler is a function that turns an incoming
     * transmission into an outgoing transmission.
     * May return null.
     */
    Send apply(Receive receive);
}
