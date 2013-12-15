package kafka.log;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import kafka.common.InvalidOffsetException;
import kafka.utils.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static kafka.utils.Utils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OffsetIndexTest {
    OffsetIndex idx = null;

    @Before
    public void setup() {
        this.idx = new OffsetIndex(nonExistantTempFile(),/* baseOffset =*/ 45L,/* maxIndexSize = */30 * 8);
    }

    @After
    public void teardown() {
        if (this.idx != null)
            this.idx.file.delete();
    }

    @Test
    public void randomLookupTest() {
        assertEquals("Not present value should return physical offset 0.", new OffsetPosition(idx.baseOffset, 0), idx.lookup(92L));

        // append some random values
        int base = (int) (idx.baseOffset + 1);
        int size = idx.maxEntries;
        List<Tuple2<Long, Integer>> vals = zip(monotonicSeq(base, size), monotonicSeq(0, size), new Function2<Integer, Integer, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> apply(Integer arg1, Integer arg2) {
                return Tuple2.make((long) arg1, arg2);
            }
        });

        foreach(vals, new Callable1<Tuple2<Long, Integer>>() {
            @Override
            public void apply(Tuple2<Long, Integer> x) {
                idx.append(x._1, x._2);
            }
        });

        // should be able to find all those values
        for (Tuple2<Long, Integer> item : vals) {
            long logical = item._1;
            int physical = item._2;

            assertEquals("Should be able to find values that are present.", new OffsetPosition(logical, physical), idx.lookup(logical));
        }

        // for non-present values we should find the offset of the largest value less than or equal to this
        final TreeMap<Long, Tuple2<Long, Integer>> valMap = Maps.newTreeMap();
        foreach(vals, new Callable1<Tuple2<Long, Integer>>() {
            @Override
            public void apply(Tuple2<Long, Integer> p) {
                valMap.put(p._1, p);
            }
        });

        List<Long> offsets = flatList(idx.baseOffset, last(vals)._1, new Function1<Long, Long>() {
            @Override
            public Long apply(Long arg) {
                return arg;
            }
        });

        Collections.shuffle(offsets);
        for (Long offset : Utils.take(offsets, 30)) {
            OffsetPosition rightAnswer =
                    (offset < valMap.firstKey())
                            ? new OffsetPosition(idx.baseOffset, 0)
                            : new OffsetPosition(valMap.floorEntry(offset).getKey(), valMap.floorEntry(offset).getValue()._2);
            assertEquals("The index should give the same answer as the sorted map", rightAnswer, idx.lookup(offset));
        }
    }

    @Test
    public void lookupExtremeCases() {
        assertEquals("Lookup on empty file", new OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
        for (int i = 0; i < idx.maxEntries; ++i)
            idx.append(idx.baseOffset + i + 1, i);
        // check first and last entry
        assertEquals(new OffsetPosition(idx.baseOffset, 0), idx.lookup(idx.baseOffset));
        assertEquals(new OffsetPosition(idx.baseOffset + idx.maxEntries, idx.maxEntries - 1), idx.lookup(idx.baseOffset + idx.maxEntries));
    }

    @Test
    public void appendTooMany() {
        for (int i = 0; i < idx.maxEntries; ++i) {
            long offset = idx.baseOffset + i + 1;
            idx.append(offset, i);
        }
        assertWriteFails("Append should fail on a full index", idx, idx.maxEntries + 1, IllegalStateException.class);
    }

    @Test(expected = InvalidOffsetException.class)
    public void appendOutOfOrder() {
        idx.append(51, 0);
        idx.append(50, 1);
    }

    @Test
    public void testReopen() {
        OffsetPosition first = new OffsetPosition(51, 0);
        OffsetPosition sec = new OffsetPosition(52, 1);
        idx.append(first.offset, first.position);
        idx.append(sec.offset, sec.position);
        idx.close();

        OffsetIndex idxRo = new OffsetIndex(idx.file, idx.baseOffset);
        assertEquals(first, idxRo.lookup(first.offset));
        assertEquals(sec, idxRo.lookup(sec.offset));
        assertEquals(sec.offset, idxRo.lastOffset);
        assertEquals(2, idxRo.entries());
        assertWriteFails("Append should fail on read-only index", idxRo, 53, IllegalStateException.class);
    }

    @Test
    public void truncate() {
        OffsetIndex idx = new OffsetIndex(/*file =*/ nonExistantTempFile(), /*baseOffset =*/ 0L, /*maxIndexSize =*/ 10 * 8);
        idx.truncate();
        for (int i = 1; i < 10; ++i)
            idx.append(i, i);

        // now check the last offset after various truncate points and validate that we can still append to the index.      
        idx.truncateTo(12);
        assertEquals("Index should be unchanged by truncate past the end", new OffsetPosition(9, 9), idx.lookup(10L));
        assertEquals("9 should be the last entry in the index", 9, idx.lastOffset);

        idx.append(10, 10);
        idx.truncateTo(10);
        assertEquals("Index should be unchanged by truncate at the end", new OffsetPosition(9, 9), idx.lookup(10L));
        assertEquals("9 should be the last entry in the index", 9, idx.lastOffset);
        idx.append(10, 10);

        idx.truncateTo(9);
        assertEquals("Index should truncate off last entry", new OffsetPosition(8, 8), idx.lookup(10L));
        assertEquals("8 should be the last entry in the index", 8, idx.lastOffset);
        idx.append(9, 9);

        idx.truncateTo(5);
        assertEquals("4 should be the last entry in the index", new OffsetPosition(4, 4), idx.lookup(10L));
        assertEquals("4 should be the last entry in the index", 4, idx.lastOffset);
        idx.append(5, 5);

        idx.truncate();
        assertEquals("Full truncation should leave no entries", 0, idx.entries());
        idx.append(0, 0);
    }

    public <T> void assertWriteFails(String message, OffsetIndex idx, int offset, Class<T> klass) {
        try {
            idx.append(offset, 1);
            fail(message);
        } catch (Exception e) {
            assertEquals("Got an unexpected exception.", klass, e.getClass());
        }
    }

    public List<Integer> monotonicSeq(int base, int len) {
        Random rand = new Random(1L);
        List<Integer> vals = Lists.newArrayList();
        int last = base;
        for (int i = 0; i < len; ++i) {
            last += rand.nextInt(15) + 1;
            vals.add(last);
        }

        return vals;
    }

    public File nonExistantTempFile() {
        File file = TestUtils.tempFile();
        file.delete();
        return file;
    }
}
