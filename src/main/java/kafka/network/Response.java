package kafka.network;

import kafka.utils.SystemTime;

public class Response {
    public final int processor;
    public final Request request;
    public final Send responseSend;
    public final ResponseAction responseAction;

    public Response(int processor, Request request, Send responseSend, ResponseAction responseAction) {
        this.processor = processor;
        this.request = request;
        this.responseSend = responseSend;
        this.responseAction = responseAction;

        request.responseCompleteTimeMs = SystemTime.instance.milliseconds();
    }


    public Response(int processor, Request request, Send responseSend) {
        this(processor, request, responseSend,
                responseSend == null ? ResponseAction.NoOpAction : ResponseAction.SendAction);
    }

    public Response(Request request, Send send) {
        this(request.processor, request, send);
    }
}
