package kafka.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonTest {
    @Test
    public void testJsonEncoding() {
        assertEquals("null", Json.encode(null));
        assertEquals("1", Json.encode(1));
        assertEquals("1", Json.encode(1L));
        assertEquals("1", Json.encode((byte) 1));
        assertEquals("1", Json.encode((short) 1));
        assertEquals("1.0", Json.encode(1.0));
        assertEquals("\"str\"", Json.encode("str"));
        assertEquals("true", Json.encode(true));
        assertEquals("false", Json.encode(false));
        assertEquals("[]", Json.encode(Lists.newArrayList()));
        assertEquals("[1,2,3]", Json.encode(ImmutableList.of(1, 2, 3)));
        assertEquals("[1,\"2\",[3]]", Json.encode(ImmutableList.<Object>of(1, "2", Lists.newArrayList(3))));
        assertEquals("{}", Json.encode(Maps.newHashMap()));
        assertEquals("{\"a\":1,\"b\":2}", Json.encode(ImmutableMap.of("a", 1, "b", 2)));
        assertEquals("{\"a\":[1,2],\"c\":[3,4]}", Json.encode(ImmutableMap.of("a", Lists.newArrayList(1, 2), "c", Lists.newArrayList(3, 4))));
    }
}
