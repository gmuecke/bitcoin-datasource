package io.devcon5.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * from <a href="http://www.javaspecialists.eu/archive/Issue246.html">Java Specialists</a>
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    private final int maxEntries;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private final Statistics stats;

    public LRUCache(int initialCapacity,
                    float loadFactor,
                    int maxEntries) {
        super(initialCapacity, loadFactor, true);
        this.maxEntries = maxEntries;
        this.stats = new Statistics();
    }

    public LRUCache(int initialCapacity,
                    int maxEntries) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, maxEntries);
    }

    public LRUCache(int maxEntries) {
        this(DEFAULT_INITIAL_CAPACITY, maxEntries);
    }

    // not very useful constructor
    public LRUCache(Map<? extends K, ? extends V> m,
                    int maxEntries) {
        this(m.size(), maxEntries);
        putAll(m);
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean result = size() > maxEntries;
        if(result){
            this.stats.evictions++;
        }
        return result;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        V result = super.putIfAbsent(key, value);
        if(result == null){
            this.stats.misses++;
        } else
        if(result == value){
            this.stats.hitWait++;
        } else {
            this.stats.hits++;
        }
        return result;
    }

    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        V result = super.getOrDefault(key, defaultValue);
        if(result == defaultValue){
            this.stats.hitWait++;
        } else {
            this.stats.hits++;
        }
        return result;
    }

    public Statistics getStatistics() {

        return stats;
    }

    public class Statistics {

        private long hits = 0;
        private long misses = 0;
        private long hitWait = 0;
        private long evictions = 0;

        public long getHits() {

            return hits;
        }

        public long getMisses() {

            return misses;
        }

        public long getHitWait() {

            return hitWait;
        }

        public long getEvictions() {

            return evictions;
        }

        public long getSize(){
            return size();
        }

        @Override
        public String toString() {

            final StringBuilder sb = new StringBuilder("{");
            sb.append("size:").append(getSize());
            sb.append(", hits:").append(hits);
            sb.append(", misses:").append(misses);
            sb.append(", hitWait:").append(hitWait);
            sb.append(", eviction:=").append(evictions);
            sb.append('}');
            return sb.toString();
        }
    }
}
