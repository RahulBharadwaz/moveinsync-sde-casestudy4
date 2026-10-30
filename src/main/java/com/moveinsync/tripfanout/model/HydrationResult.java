package com.moveinsync.tripfanout.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Result of client state hydration.
 * Can be either a REPLAY of missed sequential events or a full SNAPSHOT if the client was away too long.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HydrationResult implements Serializable {

    public enum HydrationType {
        /** Client sequence was in buffer; missed events returned in sequence order. */
        REPLAY,
        /** Client sequence was evicted or too old; client must reset state with current snapshot. */
        SNAPSHOT
    }

    private HydrationType type;
    private String topic;
    private long requestedSequenceNumber;
    private long currentSequenceNumber;
    @Builder.Default
    private List<Event> missedEvents = Collections.emptyList();
    private TopicSnapshot snapshot;

    public static HydrationResult replay(String topic, long requestedSeq, long currentSeq, List<Event> missedEvents) {
        return HydrationResult.builder()
                .type(HydrationType.REPLAY)
                .topic(topic)
                .requestedSequenceNumber(requestedSeq)
                .currentSequenceNumber(currentSeq)
                .missedEvents(missedEvents != null ? missedEvents : Collections.emptyList())
                .build();
    }

    public static HydrationResult snapshot(String topic, long requestedSeq, long currentSeq, TopicSnapshot snapshot) {
        return HydrationResult.builder()
                .type(HydrationType.SNAPSHOT)
                .topic(topic)
                .requestedSequenceNumber(requestedSeq)
                .currentSequenceNumber(currentSeq)
                .snapshot(snapshot)
                .build();
    }
}

