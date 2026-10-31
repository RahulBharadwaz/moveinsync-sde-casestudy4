package com.moveinsync.tripfanout.repository.cache;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe bounded LRU cache with time-to-live (TTL) expiration.
 * Used for hot data caching (active trip metadata, recent snapshots)
 * guaranteeing bounded staleness and finite memory consumption.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
@Slf4j
public class BoundedLruCache<K, V> {

    @Getter
    private final int maxCapacity;

    @Getter
    private final long ttlMillis;

    private final Map<K, CacheEntry<V>> map;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Getter
    private final AtomicLong hitCount = new AtomicLong(0);

    @Getter
    private final AtomicLong missCount = new AtomicLong(0);

    @Getter
    private final AtomicLong evictionCount = new AtomicLong(0);

    private record CacheEntry<T>(T value, long creationTime) {
        boolean isExpired(long ttlMs) {
            return ttlMs > 0 && (System.currentTimeMillis() - creationTime) > ttlMs;
        }
    }

    public BoundedLruCache(int maxCapacity, long ttlMillis) {
        this.maxCapacity = Math.max(10, maxCapacity);
        this.ttlMillis = ttlMillis;
        this.map = new LinkedHashMap<>(maxCapacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, CacheEntry<V>> eldest) {
                boolean shouldEvict = size() > BoundedLruCache.this.maxCapacity;
                if (shouldEvict) {
                    evictionCount.incrementAndGet();
                    log.trace("Evicted eldest cache entry key=[{}]", eldest.getKey());
                }
                return shouldEvict;
            }
        };
    }

    public Optional<V> get(K key) {
        if (key == null) {
            return Optional.empty();
        }
        lock.writeLock().lock();
        try {
            CacheEntry<V> entry = map.get(key);
            if (entry == null) {
                missCount.incrementAndGet();
                return Optional.empty();
            }
            if (entry.isExpired(ttlMillis)) {
                map.remove(key);
                missCount.incrementAndGet();
                evictionCount.incrementAndGet();
                return Optional.empty();
            }
            hitCount.incrementAndGet();
            return Optional.of(entry.value());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void put(K key, V value) {
        if (key == null || value == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            map.put(key, new CacheEntry<>(value, System.currentTimeMillis()));
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void invalidate(K key) {
        if (key == null) {
            return;
        }
        lock.writeLock().lock();
        try {
            map.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
