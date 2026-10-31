package com.moveinsync.tripfanout.repository.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedLruCacheTest {

    @Test
    @DisplayName("Cache LRU: Evicts least recently accessed entries when capacity is exceeded")
    void testLruEviction() {
        BoundedLruCache<String, String> cache = new BoundedLruCache<>(10, 60000L);

        for (int i = 1; i <= 15; i++) {
            cache.put("key-" + i, "val-" + i);
        }

        // Capacity is 10, so size must be 10 and 5 oldest entries evicted
        assertThat(cache.size()).isEqualTo(10);
        assertThat(cache.getEvictionCount().get()).isEqualTo(5L);

        // First 5 keys should be evicted
        for (int i = 1; i <= 5; i++) {
            assertThat(cache.get("key-" + i)).isEmpty();
        }

        // Keys 6 to 15 should be present
        for (int i = 6; i <= 15; i++) {
            assertThat(cache.get("key-" + i)).contains("val-" + i);
        }
    }

    @Test
    @DisplayName("Cache TTL: Expired entries are evicted upon access")
    void testTtlExpiration() throws InterruptedException {
        BoundedLruCache<String, String> cache = new BoundedLruCache<>(10, 50L); // 50ms TTL

        cache.put("short-lived", "hello");
        assertThat(cache.get("short-lived")).contains("hello");

        Thread.sleep(60);

        assertThat(cache.get("short-lived")).isEmpty();
        assertThat(cache.getMissCount().get()).isPositive();
    }
}

