package dev.rahin.durable;

import java.time.Instant;

/**
 * An event as read back from the log. Sequences are dense and start at 0, so a
 * workflow's history is always 0..n with no gaps.
 */
public record Event(String workflowId, long seq, EventType type, String payload, Instant recordedAt) {
}
