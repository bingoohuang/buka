package kafka.utils;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import kafka.common.KafkaException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Pool<K, V> implements Iterable<Map.Entry<K,V>> {
    public Function<K, V> valueFactory;

    public Pool() {
    }

    public Pool(Function<K, V> valueFactory) {
        this.valueFactory = valueFactory;
    }

    private ConcurrentHashMap<K, V> pool = new ConcurrentHashMap<K, V>();
    private Object createLock = new Object();

    public Pool(Map<K, V> m) {
        this();

        for (Map.Entry<K, V> entry : m.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
    }

    public V put(K k, V v) {
        return pool.put(k, v);
    }

    public void putIfNotExists(K k, V v) {
        pool.putIfAbsent(k, v);
    }

    /**
     * Gets the value associated with the given key. If there is no associated
     * value, then create the value using the pool's value factory and return the
     * value associated with the key. The user should declare the factory method
     * as lazy if its side-effects need to be avoided.
     *
     * @param key The key to lookup.
     * @return The final value associated with the key. This may be different from
     *         the value created by the factory if another thread successfully
     *         put a value.
     */
    public V getAndMaybePut(K key) {
        if (valueFactory == null) throw new KafkaException("Empty value factory in pool.");

        V curr = pool.get(key);
        if (curr != null) return curr;

        synchronized(createLock)  {
            curr = pool.get(key);
            if (curr == null)
                pool.put(key, valueFactory.apply(key));
            return pool.get(key);
        }
    }

    public boolean contains(K id) {
        return pool.containsKey(id);
    }

    public V get(K key) {
        return pool.get(key);
    }

    public V remove(K key) {
        return pool.remove(key);
    }

    public Set<K> keys() {
        return pool.keySet();
    }

    public Collection<V> values() {
           return pool.values();
    }

    public void clear() {
        pool.clear();
    }

    public int size() {
        return pool.size();
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        final Iterator<Map.Entry<K, V>> iterator = pool.entrySet().iterator();
        return new Iterator<Map.Entry<K, V>>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Map.Entry<K, V> next() {
                return iterator.next();
            }

            @Override
            public void remove() {
            }
        };
    }

    public Map<K, V> toMap() {
        return Maps.newHashMap(pool);
    }
}
