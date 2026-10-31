package com.moveinsync.tripfanout.service;

import com.moveinsync.tripfanout.engine.GatewayQueueManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("null")
class OnCallMetricsServiceTest {

    @Mock
    private GatewayQueueManager queueManager;

    @InjectMocks
    private OnCallMetricsService metricsService;

    @Test
    @DisplayName("On-Call Telemetry: Tracks lag, errors, rejections, retries, and queue depth accurately")
    void testOnCallMetricsAggregation() {
        when(queueManager.getActiveSessionCount()).thenReturn(100);
        when(queueManager.getBackPressuredSessionCount()).thenReturn(5L);
        when(queueManager.getTotalConflatedCount()).thenReturn(500L);
        when(queueManager.getTotalDroppedCount()).thenReturn(2L);

        // Record some events
        long now = System.currentTimeMillis();
        metricsService.recordConsumerLag(now - 10);
        metricsService.recordConsumerLag(now - 25);
        metricsService.recordError();
        metricsService.recordSubscriptionRejection();
        metricsService.recordSubscriptionRejection();
        metricsService.recordHydrationRetry();

        Map<String, Object> report = metricsService.getOnCallReport();

        assertThat(report).isNotNull();
        assertThat(report.get("subscriptionRejections")).isEqualTo(2L);
        assertThat(report.get("hydrationRetries")).isEqualTo(1L);

        @SuppressWarnings("unchecked")
        Map<String, Object> errors = (Map<String, Object>) report.get("errorTracking");
        assertThat(errors.get("totalErrorCount")).isEqualTo(1L);

        @SuppressWarnings("unchecked")
        Map<String, Object> lag = (Map<String, Object>) report.get("consumerLag");
        assertThat(lag).containsKey("averageLagMs");
        assertThat(lag).containsKey("p99LagMs");

        @SuppressWarnings("unchecked")
        Map<String, Object> queue = (Map<String, Object>) report.get("queueDepth");
        assertThat(queue.get("activeSessionQueues")).isEqualTo(100);
        assertThat(queue.get("backpressuredSessions")).isEqualTo(5L);
    }
}
