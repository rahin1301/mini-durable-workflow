package dev.rahin.durable;

import java.time.Instant;

/** Header row for a workflow. Progress isn't here, it's in the event log. */
public record WorkflowInstance(
        String id,
        String type,
        String input,
        WorkflowStatus status,
        Instant createdAt,
        Instant updatedAt) {
}
