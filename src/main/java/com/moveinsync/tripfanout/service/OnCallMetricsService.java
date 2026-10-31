package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 3 AM On-Call Observability Service.
 * Tracks and exposes the exact production telemetry counters an on-call engineer needs
 * during an incident: consumer lag, error rate, subscription rejections, hydration retries,
 * and queue depth.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OnCallMetricsService {

    private final GatewayQueueManager queueManager;

    // Error and failure telemetry
    private final AtomicLong totalErrorCount = new AtomicLong(0);
    private final AtomicLong subscriptionRejections = new AtomicLong(0);
    private final AtomicLong hydrationRetries = new AtomicLong(0);

    // Sliding window of recent consumer latencies/lags (in milliseconds)
    private static final int MAX_LAG_SAMPLES = 1000;
    private final Queue<Double> recentLagSamples = new ConcurrentLinkedQueue<>();

    private final long serviceStartTime = System.currentTimeMillis();

    /**
     * Record consumer lag when a message is drained from an outbound queue.
     * Lag = System.currentTimeMillis() - event.getTimestamp().
     */
    public void recordConsumerLag(long eventTimestamp) {
        if (eventTimestamp <= 0) {
            return;
        }
        double lagMs = Math.max(0, System.currentTimeMillis() - eventTimestamp);
        recentLagSamples.offer(lagMs);
        while (recentLagSamples.size() > MAX_LAG_SAMPLES) {
            recentLagSamples.poll();
        }
    }

    public void recordError() {
        totalErrorCount.incrementAndGet();
    }

    public void recordSubscriptionRejection() {
        subscriptionRejections.incrementAndGet();
    }

    public void recordHydrationRetry() {
        hydrationRetries.incrementAndGet();
    }

    /**
     * Aggregates all on-call counters into a structured report.
     */
    public Map<String, Object> getOnCallReport() {
        Map<String, Object> report = new LinkedHashMap<>();

        // 1. Consumer Lag Metrics
        List<Double> lags = new ArrayList<>(recentLagSamples);
        double avgLag = 0.0;
        double maxLag = 0.0;
        double p99Lag = 0.0;

        if (!lags.isEmpty()) {
            Collections.sort(lags);
            avgLag = lags.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            maxLag = lags.get(lags.size() - 1);
            int p99Idx = (int) Math.ceil(0.99 * lags.size()) - 1;
            p99Lag = lags.get(Math.max(0, Math.min(p99Idx, lags.size() - 1)));
        }

        Map<String, Object> lagMetrics = new LinkedHashMap<>();
        lagMetrics.put("averageLagMs", Math.round(avgLag * 100.0) / 100.0);
        lagMetrics.put("maxLagMs", Math.round(maxLag * 100.0) / 100.0);
        lagMetrics.put("p99LagMs", Math.round(p99Lag * 100.0) / 100.0);
        report.put("consumerLag", lagMetrics);

        // 2. Error Rate & Counters
        long uptimeSeconds = Math.max(1, (System.currentTimeMillis() - serviceStartTime) / 1000);
        double errorRatePerMinute = (totalErrorCount.get() * 60.0) / uptimeSeconds;

        Map<String, Object> errorMetrics = new LinkedHashMap<>();
        errorMetrics.put("totalErrorCount", totalErrorCount.get());
        errorMetrics.put("errorRatePerMinute", Math.round(errorRatePerMinute * 100.0) / 100.0);
        report.put("errorTracking", errorMetrics);

        // 3. Security Rejections (Cross-tenant & unauthorized blocks)
        report.put("subscriptionRejections", subscriptionRejections.get());

        // 4. Hydration Retries (Reconnect resync requests)
        report.put("hydrationRetries", hydrationRetries.get());

        // 5. Queue Depth & Backpressure Metrics
        int activeSessions = queueManager.getActiveSessionCount();
        long backpressured = queueManager.getBackPressuredSessionCount();

        Map<String, Object> queueMetrics = new LinkedHashMap<>();
        queueMetrics.put("activeSessionQueues", activeSessions);
        queueMetrics.put("backpressuredSessions", backpressured);
        queueMetrics.put("totalConflatedLocations", queueManager.getTotalConflatedCount());
        queueMetrics.put("totalDroppedLocations", queueManager.getTotalDroppedCount());
        report.put("queueDepth", queueMetrics);

        report.put("uptimeSeconds", uptimeSeconds);

        return report;
    }
}
