package kafka.utils;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class IteratorTemplateTest {

    List<Integer> lst = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    IteratorTemplate iterator = new IteratorTemplate<Integer>() {
        int i = 0;

        @Override
        public Integer makeNext() {
            if (i >= lst.size()) {
                return allDone();
            } else {
                Integer item = lst.get(i);
                i += 1;
                return item;
            }
        }
    };

    @Test
    public void testIterator() {
        for (int i = 0; i < 10; ++i) {
            assertEquals("We should have an item to read.", true, iterator.hasNext());
            assertEquals("Checking again shouldn't change anything.", true, iterator.hasNext());
            assertEquals("Peeking at the item should show the right thing.", i, iterator.peek());
            assertEquals("Peeking again shouldn't change anything", i, iterator.peek());
            assertEquals("Getting the item should give the right thing.", i, iterator.next());
        }
        assertEquals("All gone!", false, iterator.hasNext());

        try {
            iterator.peek();
            fail();
        } catch (NoSuchElementException e) {

        }

        try {
            iterator.next();
            fail();
        } catch (NoSuchElementException e) {

        }

    }

}
