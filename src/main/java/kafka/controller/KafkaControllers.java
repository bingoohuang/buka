package kafka.controller;

import com.alibaba.fastjson.JSONObject;
import kafka.common.KafkaException;
import kafka.utils.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaControllers {
    public static final String MBeanName = "kafka.controller:type=KafkaController,name=ControllerOps";
    public static final String stateChangeLogger = "state.change.logger";
    public static final int InitialControllerEpoch = 1;
    public static final int InitialControllerEpochZkVersion = 1;

    static Logger logger = LoggerFactory.getLogger(KafkaControllers.class);

    public static int parseControllerId(String controllerInfoString) {
        try {
            JSONObject controllerInfo = Json.parseFull(controllerInfoString);
            if (controllerInfo == null)
                throw new KafkaException("Failed to parse the controller info json [%s].", controllerInfoString);

            return controllerInfo.getIntValue("brokerid");
        } catch (Throwable t) {
            // It may be due to an incompatible controller register version
            logger.warn("Failed to parse the controller info as json. "
                    + "Probably this controller is still using the old " +
                    "format [{}] to store the broker id in zookeeper", controllerInfoString);
            try {
                return Integer.parseInt(controllerInfoString);
            } catch (Throwable t1) {
                throw new KafkaException(t1,
                        "Failed to parse the controller info: " + controllerInfoString
                                + ". This is neither the new or the old format.");
            }
        }
    }
}
