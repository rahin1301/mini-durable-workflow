package dev.rahin.durable;

public enum EventType {
    // Commands: produced by the workflow code itself. Replay has to produce the same
    // ones in the same order, otherwise the workflow is non-deterministic.
    TIMER_STARTED(true, false),
    TIMER_CANCELLED(true, false),
    ACTIVITY_SCHEDULED(true, false),
    SIDE_EFFECT_RECORDED(true, false),
    WORKFLOW_COMPLETED(true, true),
    WORKFLOW_FAILED(true, true),

    // External: delivered from outside the replay loop. Replay reads these back and
    // never re-runs them, which is what stops an activity firing twice on recovery.
    WORKFLOW_STARTED(false, false),
    TIMER_FIRED(false, false),
    ACTIVITY_COMPLETED(false, false),
    ACTIVITY_FAILED(false, false),
    SIGNAL_RECEIVED(false, false);

    private final boolean command;
    private final boolean terminal;

    EventType(boolean command, boolean terminal) {
        this.command = command;
        this.terminal = terminal;
    }

    public boolean isCommand() {
        return command;
    }

    public boolean isTerminal() {
        return terminal;
    }
}
