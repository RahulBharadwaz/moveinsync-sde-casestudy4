package com.moveinsync.tripfanout.engine;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration policy for connection-level back-pressure, queue bounding, and conflation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Component
@ConfigurationProperties(prefix = "app.queue")
public class SlowConsumerPolicy {

    /**
     * Maximum capacity of pending events in a client's outbound queue before back-pressure kicks in.
     */
    @Builder.Default
    private int maxCapacity = 50;

    /**
     * Whether location updates are conflated to only keep the latest location per cab.
     */
    @Builder.Default
    private boolean conflationEnabled = true;

    /**
     * Threshold (ratio of maxCapacity) at which a consumer is flagged as back-pressured (e.g. 0.8 = 80%).
     */
    @Builder.Default
    private double backPressureThreshold = 0.8;
}

