package com.moveinsync.tripfanout.websocket;

public final class TopicUtils {

    private TopicUtils() {}

    /**
     * Converts a destination or raw topic into canonical format "type:id" (e.g. "trip:TRIP-101").
     * Supports:
     * - /topic/trip/TRIP-101 -> trip:TRIP-101
     * - /topic/trips/TRIP-101 -> trip:TRIP-101
     * - /topic/office/OFFICE-BLR -> office:OFFICE-BLR
     * - /topic/cab/CAB-01 -> cab:CAB-01
     * - /topic/trip:TRIP-101 -> trip:TRIP-101
     * - trip:TRIP-101 -> trip:TRIP-101
     */
    public static String toCanonicalTopic(String destination) {
        if (destination == null || destination.isBlank()) {
            return "";
        }
        String clean = destination.trim();
        if (clean.startsWith("/topic/")) {
            clean = clean.substring(7);
        } else if (clean.startsWith("/app/")) {
            clean = clean.substring(5);
        } else if (clean.startsWith("/")) {
            clean = clean.substring(1);
        }

        // Handle colon-separated (e.g. "trip:TRIP-101")
        if (clean.contains(":")) {
            return clean;
        }

        // Handle slash-separated (e.g. "trip/TRIP-101" or "trips/TRIP-101")
        String[] parts = clean.split("/", 2);
        if (parts.length == 2) {
            String type = parts[0].toLowerCase();
            if (type.endsWith("s")) {
                type = type.substring(0, type.length() - 1); // "trips" -> "trip"
            }
            return type + ":" + parts[1];
        }

        return clean;
    }

    /**
     * Converts a canonical topic "type:id" (e.g. "trip:TRIP-101") to STOMP destination "/topic/trip/TRIP-101".
     */
    public static String toDestination(String topic) {
        if (topic == null || topic.isBlank()) {
            return "/topic";
        }
        if (topic.contains(":")) {
            String[] parts = topic.split(":", 2);
            return "/topic/" + parts[0] + "/" + parts[1];
        }
        return "/topic/" + topic;
    }

    public static String extractType(String canonicalTopic) {
        if (canonicalTopic != null && canonicalTopic.contains(":")) {
            return canonicalTopic.split(":", 2)[0].toLowerCase();
        }
        return "";
    }

    public static String extractId(String canonicalTopic) {
        if (canonicalTopic != null && canonicalTopic.contains(":")) {
            return canonicalTopic.split(":", 2)[1];
        }
        return canonicalTopic != null ? canonicalTopic : "";
    }
}

