package dev.rahin.durable;

import dev.rahin.durable.EventStore.NewEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Drives workflows forward.
 *
 * <p>A decision is: read the history, replay the workflow code against it, collect the
 * commands the code wants, and commit them in one transaction. Then run whatever side
 * effects those commands asked for, record the results, and decide again. Repeat until
 * the workflow finishes or has nothing left it can act on.
 *
 * <p>Nothing about a workflow is cached between decisions, so resuming one after a restart
 * is the same code path as running it normally. {@link #recover()} is just a loop over the
 * workflows that were still RUNNING.
 */
public class WorkflowEngine {

    // Nothing legitimate needs this many rounds; it's here so a bug in workflow code
    // fails loudly instead of spinning.
    private static final int MAX_ROUNDS = 1000;

    private final EventStore store;
    private final Map<String, Workflow> workflows = new HashMap<>();
    private final Map<String, Activity> activities = new HashMap<>();

    public WorkflowEngine(EventStore store) {
        this.store = store;
    }

    public void registerWorkflow(String type, Workflow workflow) {
        workflows.put(type, workflow);
    }

    public void registerActivity(String name, Activity activity) {
        activities.put(name, activity);
    }

    /** Starts a workflow and drives it as far as it will go. */
    public void start(String workflowId, String type, String input) {
        if (!workflows.containsKey(type)) {
            throw new IllegalArgumentException("no workflow registered as " + type);
        }
        store.createWorkflow(workflowId, type, input);
        drive(workflowId);
    }

    /**
     * Delivers a signal and drives the workflow on. Returns false if the workflow had
     * already finished, which happens a lot when events keep arriving for a user whose
     * journey is over.
     */
    public boolean signal(String workflowId, String name, String payload) {
        if (store.find(workflowId).map(WorkflowInstance::status).orElse(null) != WorkflowStatus.RUNNING) {
            return false;
        }
        NewEvent event = new NewEvent(EventType.SIGNAL_RECEIVED, Payload.encode(name, payload));
        for (int attempt = 0; attempt < 5; attempt++) {
            try {
                store.append(workflowId, store.nextSequence(workflowId), List.of(event));
                drive(workflowId);
                return true;
            } catch (ConflictException retry) {
                // something else appended between the read and the write; losing a user
                // event would be worse than trying again
            } catch (StoreException e) {
                return false; // finished underneath us
            }
        }
        throw new StoreException("could not deliver signal " + name + " to " + workflowId);
    }

    /** Re-drives everything that was still running, e.g. after a restart. */
    public void recover() {
        for (String workflowId : store.idsWithStatus(WorkflowStatus.RUNNING)) {
            drive(workflowId);
        }
    }

    /** Runs decisions and activities alternately until the workflow finishes or blocks. */
    public void drive(String workflowId) {
        for (int round = 0; round < MAX_ROUNDS; round++) {
            boolean decided = decide(workflowId);
            if (isFinished(workflowId)) {
                return;
            }
            boolean ranActivity = runPendingActivities(workflowId);
            if (!decided && !ranActivity) {
                return; // waiting on something this phase can't produce
            }
        }
        throw new IllegalStateException(workflowId + " still going after " + MAX_ROUNDS + " rounds");
    }

    /** One decision. Returns true if it appended anything. */
    boolean decide(String workflowId) {
        List<Event> history = store.readHistory(workflowId);
        if (history.isEmpty()) {
            throw new StoreException("no such workflow: " + workflowId);
        }
        WorkflowInstance instance = store.find(workflowId).orElseThrow();
        if (instance.status() != WorkflowStatus.RUNNING) {
            // Closed history. Replaying it would re-derive commands the workflow code
            // never issued itself, like WORKFLOW_COMPLETED.
            return false;
        }
        Workflow workflow = workflows.get(instance.type());
        if (workflow == null) {
            throw new IllegalStateException("no workflow registered as " + instance.type());
        }

        WorkflowContext ctx = new WorkflowContext(history);
        List<NewEvent> commands = new ArrayList<>();
        WorkflowStatus status = null;

        try {
            workflow.run(ctx, history.get(0).payload());
            if (ctx.hasUnconsumedCommands()) {
                throw new NonDeterministicException(-1, "more commands", "a workflow that finished early");
            }
            commands.addAll(ctx.pendingCommands());
            commands.add(NewEvent.of(EventType.WORKFLOW_COMPLETED));
            status = WorkflowStatus.COMPLETED;
        } catch (Suspend suspend) {
            commands.addAll(ctx.pendingCommands());
        } catch (NonDeterministicException e) {
            // Not the workflow's fault and not something a terminal event should record.
            // The history is fine, the code no longer matches it; park the workflow so it
            // can be picked up again once the code is put back.
            throw e;
        } catch (RuntimeException failure) {
            // The workflow threw for real. Record why and stop; retrying deterministic
            // code against the same history would only fail the same way.
            commands.clear();
            commands.add(new NewEvent(EventType.WORKFLOW_FAILED, failure.toString()));
            status = WorkflowStatus.FAILED;
        }

        if (commands.isEmpty()) {
            return false;
        }
        try {
            store.append(workflowId, history.size(), commands, status);
        } catch (ConflictException stale) {
            // Another process moved this workflow along while we were deciding, so this
            // decision was made against a history that no longer exists. Drop it.
            return false;
        }
        return true;
    }

    /**
     * Runs any activity that's been scheduled but has no result yet.
     *
     * <p>The activity runs before its result is recorded, so a crash in between means it
     * runs again on restart. That's at-least-once, and it's the honest trade: the
     * alternative is recording first and losing the activity entirely if we die before
     * running it. Activities need to be idempotent, or carry a key the far side can
     * deduplicate on.
     */
    private boolean runPendingActivities(String workflowId) {
        List<Event> history = store.readHistory(workflowId);
        Set<String> answered = new HashSet<>();
        for (Event event : history) {
            if (event.type() == EventType.ACTIVITY_COMPLETED || event.type() == EventType.ACTIVITY_FAILED) {
                answered.add(Payload.head(event.payload()));
            }
        }

        boolean ranAnything = false;
        for (Event event : history) {
            if (event.type() != EventType.ACTIVITY_SCHEDULED || answered.contains(Long.toString(event.seq()))) {
                continue;
            }
            NewEvent result = run(event);
            try {
                store.append(workflowId, store.nextSequence(workflowId), List.of(result));
            } catch (ConflictException stale) {
                continue; // someone else is driving this one
            }
            ranAnything = true;
        }
        return ranAnything;
    }

    private NewEvent run(Event scheduled) {
        String name = Payload.head(scheduled.payload());
        Activity activity = activities.get(name);
        if (activity == null) {
            return new NewEvent(
                    EventType.ACTIVITY_FAILED, Payload.encode(scheduled.seq(), "no activity registered as " + name));
        }
        try {
            String result = activity.run(Payload.tail(scheduled.payload()));
            return new NewEvent(EventType.ACTIVITY_COMPLETED, Payload.encode(scheduled.seq(), result));
        } catch (Exception e) {
            return new NewEvent(EventType.ACTIVITY_FAILED, Payload.encode(scheduled.seq(), e.toString()));
        }
    }

    private boolean isFinished(String workflowId) {
        return store.find(workflowId).orElseThrow().status() != WorkflowStatus.RUNNING;
    }
}
