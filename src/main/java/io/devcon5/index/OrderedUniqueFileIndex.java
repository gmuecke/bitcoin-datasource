package io.devcon5.index;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * An index for offset positions in a file. The index associates an orderable, unique key to such offset position
 */
public class OrderedUniqueFileIndex<KEY extends Comparable> {

    /**
     * The actual index
     */
    private TreeMap<KEY, Long> index = new TreeMap<>();

    /**
     * Returns the offset for the specified key. If the key itself is not contained in the index, the offset for the next lower
     * entry or 0 is returned.
     *
     * @param key
     *  the key to search for in the index
     *
     * @return
     *  the file offset position for the requested key
     */
    public long offsetFor(KEY key) {

        return Optional.of(index.get(key))
                       .orElseGet(() -> Optional.of(index.lowerEntry(key)).map(Map.Entry::getValue).orElse(0L));
    }
}
