package kafka.utils;

import com.google.common.base.Charsets;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class ZKStringSerializer implements ZkSerializer {
    public static ZkSerializer instance = new ZKStringSerializer();

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return ((String) data).getBytes(Charsets.UTF_8);

    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        if (bytes == null) return null;

        return new String(bytes, Charsets.UTF_8);
    }
}
