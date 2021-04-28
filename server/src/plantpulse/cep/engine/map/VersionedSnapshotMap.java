package plantpulse.cep.engine.map;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


public class VersionedSnapshotMap<K,V> {

    private static class Holder<T> {
        private boolean live = true;
        private T value = null;
    }

    private class PassThrough {
        public V put(K key, V val) {
            return  store.put(key,val);
        }
        public V remove(K key) {
            return  store.remove(key);
        }
        public V get(K key) {
            return store.get(key);
        }
    }

    private class LoggedThrough extends PassThrough {

        private final ConcurrentHashMap<K, Holder<V>> diff = new ConcurrentHashMap<>();
        private final AtomicBoolean record = new AtomicBoolean(true);

        @Override
        public V get(K key) {
            // no need to worry about recording things....
            Holder<V> holder = diff.get(key);
            if (holder != null) {
                synchronized(holder) {
                    if (holder.live) {
                        return holder.value;
                    }
                }
            }
            // no race condition, the get can safely get the old value even
            // if a new holder was created in the race.
            return store.get(key);
        }

        private V undercover(K key, V val, boolean remove) {
            if (!record.get()) {
                // recording complete for this logger.
                // either the Holder has:
                //   1. never been created
                //   2. created, but not yet written back
                //   3. created, and written back
                //   4. created, written back, and removed.
                final Holder<V> holder = diff.get(key);
                if (holder != null) {
                    // 2, or 3.
                    synchronized (holder) {
                        if (holder.live) {
                            V prev = holder.value;
                            holder.value = null;
                            // push back this Holder, and mark it dead.
                            // subsequent calls will find it gone...
                            store.put(key, val);
                            holder.live = false;
                            return prev;
                        }
                    }
                }
                // 1, 3, or 4.
                if (remove) {
                    return store.remove(key);
                }
                return store.put(key, val);
            }

            // we are still recording...
            // optimistically create a new Holder.
            // we will have to discard this if another thread has already done one.
            Holder<V> nref = new Holder<>();
            nref.value = store.get(key);

            // yes, put it on the queue even if the recording may have stopped.
            Holder<V> race = diff.putIfAbsent(key, nref);

            // holder becomes whatever instance was first registered for this key.
            Holder<V> holder = race == null ? nref : race;

            synchronized(holder) {
                if (holder.live) {
                    V prev = holder.value;
                    holder.value = val;
                    if (!record.get()) {
                        // we thought we were recording, but that
                        // changed in a race condition. We push our value
                        // back through to the source.
                        holder.live = false;
                        holder.value = null;
                        diff.remove(key);
                        if (remove) {
                            store.remove(key);
                        } else {
                            store.put(key, val);
                        }
                    }
                    return prev;
                }
            }
            if (remove) {
                return store.remove(key);
            }
            return store.put(key, val);
        }

        @Override
        public V put(K key, V val) {
            return undercover(key, val, false);
        }

        @Override
        public V remove(K key) {
            return undercover(key, null, true);
        }

        public void flush() {
            // OK, recordings are no longer applied
            record.set(false);
            while (!diff.isEmpty()) {
                Iterator<Map.Entry<K, Holder<V>>> it = diff.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<K, Holder<V>> entry = it.next();
                    Holder<V> holder = entry.getValue();
                    K key = entry.getKey();
                    synchronized (holder) {
                        if (holder.live) {
                            holder.live = false;
                            if (holder.value  != null) {
                                store.put(key, holder.value);
                            } else {
                                store.remove(key);
                            }
                            holder.value = null;
                        }
                    }
                    it.remove();
                }
            }
        }

    }

    private final PassThrough simplepass = new PassThrough();
    private final ConcurrentHashMap<K, V> store = new ConcurrentHashMap<>();
    private final AtomicReference<PassThrough> core = new AtomicReference<>(simplepass);

    public V get(K key) {
        return core.get().get(key);
    }

    public V put(K key, V val)  {
        return core.get().put(key, val);
    }

    public V remove(K key) {
        return core.get().remove(key);
    }    

    public void snapshot(BiConsumer<? super K, ? super V> action) {
        LoggedThrough logged = new LoggedThrough();
        if (core.compareAndSet(simplepass, logged)) {
            try {
                store.forEach(action);
            } finally {
                logged.flush();
                if (!core.compareAndSet(logged, simplepass)) {
                    throw new IllegalStateException("Unable to restore the simple passthrough");
                }
            }
        } else  {
            throw new IllegalStateException("Only one snapshot at a time, please");
        }
    }

}