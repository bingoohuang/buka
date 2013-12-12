package kafka.api;

import com.google.common.collect.ImmutableMap;
import kafka.common.ErrorMapping;
import kafka.utils.*;

import java.nio.ByteBuffer;
import java.util.Map;

import static kafka.api.ApiUtils.readShortString;
import static kafka.api.ApiUtils.writeShortString;

public class LeaderAndIsrResponse extends RequestOrResponse {
    public static LeaderAndIsrResponse readFrom(final ByteBuffer buffer) {
        int correlationId = buffer.getInt();
        short errorCode = buffer.getShort();
        int numEntries = buffer.getInt();
        Map<Tuple2<String, Integer>, Short> responseMap = Utils.flatMap(0, numEntries, new Function0<Map<Tuple2<String, Integer>, Short>>() {
            @Override
            public Map<Tuple2<String, Integer>, Short> apply() {
                String topic = readShortString(buffer);
                int partition = buffer.getInt();
                short partitionErrorCode = buffer.getShort();
                return ImmutableMap.of(Tuple2.make(topic, partition), partitionErrorCode);
            }
        });

        return new LeaderAndIsrResponse(correlationId, responseMap, errorCode);
    }

    public Map<Tuple2<String, Integer>, Short> responseMap;
    public short errorCode;

    public LeaderAndIsrResponse(int correlationId, Map<Tuple2<String, Integer>, Short> responseMap) {
        this(correlationId, responseMap, ErrorMapping.NoError);
    }

    public LeaderAndIsrResponse(int correlationId, Map<Tuple2<String, Integer>, Short> responseMap, short errorCode) {
        super(correlationId);
        this.responseMap = responseMap;
        this.errorCode = errorCode;
    }

    @Override
    public int sizeInBytes() {
        return 4 /* correlation id */ +
                2 /* error code */ +
                4 /* number of responses */ +
                Utils.foldLeft(responseMap, 0, new Function3<Integer, Tuple2<String, Integer>, Short, Integer>() {
                    @Override
                    public Integer apply(Integer arg1, Tuple2<String, Integer> key, Short arg3) {
                        return arg1 + 2 + key._1.length() /* topic */ +
                                4 /* partition */ +
                                2 /* error code for this partition */;
                    }
                });
    }

    @Override
    public void writeTo(final ByteBuffer buffer) {
        buffer.putInt(correlationId);
        buffer.putShort(errorCode);
        buffer.putInt(responseMap.size());

        Utils.foreach(responseMap, new Function2<Tuple2<String, Integer>, Short, Void>() {
            @Override
            public Void apply(Tuple2<String, Integer> key, Short value) {
                writeShortString(buffer, key._1);
                buffer.putInt(key._2);
                buffer.putShort(value);

                return null;
            }
        });
    }
}
