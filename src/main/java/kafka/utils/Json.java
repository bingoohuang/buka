package kafka.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.common.KafkaException;

/**
 * A wrapper that synchronizes JSON in scala, which is not threadsafe.
 */
public class Json {
    public static Object lock = new Object();

    /**
     * Parse a JSON string into an object
     */
    public static JSONObject parseFull(String input) {
        synchronized (lock) {
            try {
                return JSON.parseObject(input);
            } catch (Throwable t) {
                throw new KafkaException(t, "Can't parse json string: %s", input);
            }
        }
    }

    /**
     * Encode an object into a JSON string. This method accepts any type T where
     * T => null | Boolean | String | Number | Map[String, T] | Array[T] | Iterable[T]
     * Any other type will result in an exception.
     * <p/>
     * This method does not properly handle non-ascii characters.
     */
    public static String encode(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof Boolean) return obj.toString();
        if (obj instanceof String) return "\"" + obj + "\"";
        if (obj instanceof Number) return obj.toString();

        return JSON.toJSONString(obj);
    }
}
