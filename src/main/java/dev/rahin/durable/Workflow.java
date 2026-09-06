package dev.rahin.durable;

/**
 * Workflow code. This gets re-executed from the top every time the engine needs to work
 * out what happens next, so it has to be deterministic: no clocks, no random, no I/O, no
 * reading anything that might have changed. Anything like that belongs in an activity.
 */
public interface Workflow {

    void run(WorkflowContext ctx, String input);
}
