package dev.rahin.durable;

/**
 * Thrown when a workflow id is started twice. Callers can catch this and treat it as a
 * no-op so a redelivered "cart created" message doesn't start a second workflow.
 */
public class WorkflowExistsException extends StoreException {

    public WorkflowExistsException(String workflowId) {
        super("workflow already exists: " + workflowId);
    }
}
