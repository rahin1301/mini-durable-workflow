package dev.rahin.durable;

/**
 * Thrown when an append is based on a history that has since moved on. The caller's
 * decision was made against a history that no longer exists, so it has to be thrown
 * away and recomputed rather than merged. Nothing is written when this is thrown.
 */
public class ConflictException extends StoreException {

    public ConflictException(String workflowId, long expected, long actual) {
        super("append to " + workflowId + " expected seq " + expected + " but history is at " + actual);
    }
}
