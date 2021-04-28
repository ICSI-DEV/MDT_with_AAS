package plantpulse.cep.engine.map;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * A snapshotable map.
 */
public class SnapshotMap<K, V> {
	
	private static final Log log = LogFactory.getLog(SnapshotMap.class);
	
	
    // stores recent updates
    volatile Map<K, V> updates;
    volatile Map<K, V> updatesToMerge;
    // map stores all snapshot data
    volatile NavigableMap<K, V> snapshot;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SnapshotMap() {
        updates = new ConcurrentHashMap<K, V>();
        updatesToMerge = new ConcurrentHashMap<K, V>();
        snapshot = new ConcurrentSkipListMap<K, V>();
    }

    /**
     * Create a snapshot of current map.
     *
     * @return a snapshot of current map.
     */
    public NavigableMap<K, V> snapshot() {
    	long start = System.currentTimeMillis();
        this.lock.writeLock().lock();
        try {
            if (updates.isEmpty()) {
                return snapshot;
            }
            // put updates for merge to snapshot
            updatesToMerge = updates;
            updates = new ConcurrentHashMap<K, V>();
        } finally {
            this.lock.writeLock().unlock();
        }
        // merging the updates to snapshot
        for (Map.Entry<K, V> entry : updatesToMerge.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue());
        }
        // clear updatesToMerge
        this.lock.writeLock().lock();
        try {
            updatesToMerge = new ConcurrentHashMap<K, V>();
        } finally {
            this.lock.writeLock().unlock();
        }
        long end = System.currentTimeMillis() - start;
        if(end > 100) {
        	log.warn("Map snapshot is very slow : snapshot_time=[" + end + "]ms");
        }
        return snapshot;
    }

    /**
     * Associates the specified value with the specified key in this map.
     *
     * @param key
     *          Key with which the specified value is to be associated.
     * @param value
     *          Value to be associated with the specified key.
     */
    public void put(K key, V value) {
        this.lock.readLock().lock();
        try {
            updates.put(key, value);
        } finally {
            this.lock.readLock().unlock();
        }

    }

    /**
     * Removes the mapping for the key from this map if it is present.
     *
     * @param key
     *          Key whose mapping is to be removed from this map.
     */
    public void remove(K key) {
        this.lock.readLock().lock();
        try {
            // first remove updates
            updates.remove(key);
            updatesToMerge.remove(key);
            // then remove snapshot
            snapshot.remove(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
     * Returns true if this map contains a mapping for the specified key.
     *
     * @param key
     *          Key whose presence is in the map to be tested.
     * @return true if the map contains a mapping for the specified key.
     */
    public boolean containsKey(K key) {
        this.lock.readLock().lock();
        try {
            return updates.containsKey(key)
                 | updatesToMerge.containsKey(key)
                 | snapshot.containsKey(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}