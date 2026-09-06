package dev.rahin.durable;

import dev.rahin.durable.EventStore.NewEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * What workflow code is allowed to talk to. One of these is built per decision, wrapping
 * the history read at that moment.
 *
 * <p>Calls like {@link #executeActivity} are answered from history when the answer is
 * already there. When it isn't, the call records what it wants and unwinds the workflow
 * with {@link Suspend}: the engine commits the new command, does the work outside the
 * replay loop, and runs the workflow again from the top. So the workflow function may
 * execute many times, but each activity in it runs once.
 *
 * <p>Temporal parks a coroutine at that point instead of unwinding, which means it doesn't
 * re-execute the earlier part of the workflow. Re-running is simpler and behaves the same
 * as long as workflow code is genuinely deterministic and cheap, which it has to be anyway.
 */
public class WorkflowContext {

    private final List<Event> commands;
    private final List<Event> signals;
    private final List<Event> history;
    private final List<NewEvent> pending = new ArrayList<>();
    private final Map<String, Consumer<String>> signalHandlers = new HashMap<>();
    private int commandIndex;
    private int signalCursor;

    WorkflowContext(List<Event> history) {
        this.history = history;
        this.commands = history.stream().filter(e -> e.type().isCommand()).toList();
        this.signals = history.stream().filter(e -> e.type() == EventType.SIGNAL_RECEIVED).toList();
    }

    /**
     * True while the engine is re-deriving decisions already in the history. Handy for not
     * logging the same line on every replay; Temporal exposes the same flag.
     */
    public boolean isReplaying() {
        return commandIndex < commands.size();
    }

    /**
     * Runs a registered activity and returns its result. On the first pass this schedules
     * the activity and unwinds; on later passes it returns the recorded result.
     *
     * @throws ActivityFailedException if the activity ended in failure
     */
    public String executeActivity(String name, String input) {
        Event recorded = nextCommand();

        if (recorded == null) {
            pending.add(new NewEvent(EventType.ACTIVITY_SCHEDULED, Payload.encode(name, input)));
            throw new Suspend();
        }
        if (recorded.type() != EventType.ACTIVITY_SCHEDULED) {
            throw new NonDeterministicException(commandIndex - 1, recorded.type().name(), "activity " + name);
        }
        String recordedName = Payload.head(recorded.payload());
        if (!recordedName.equals(name)) {
            throw new NonDeterministicException(
                    commandIndex - 1, "activity " + recordedName, "activity " + name);
        }

        // The activity id is the sequence its ACTIVITY_SCHEDULED landed at, so the result
        // can point back at exactly the call it answers.
        Event result = resultFor(recorded.seq());
        if (result == null) {
            throw new Suspend();
        }
        if (result.type() == EventType.ACTIVITY_FAILED) {
            throw new ActivityFailedException(name, Payload.tail(result.payload()));
        }
        return Payload.tail(result.payload());
    }

    /**
     * Waits for a stretch of wall-clock time. This is a timer event in the log, not a
     * sleep: the workflow unwinds here and the process is free to exit. Whatever picks
     * the log up next, this process or one started tomorrow, sees a timer with a fire
     * time and carries on from there.
     */
    public void sleep(Duration duration) {
        Event recorded = nextCommand();

        if (recorded == null) {
            // Reading the clock is only allowed because the answer is written into the
            // event right now and read back from there on every later pass. Calling
            // Instant.now() during replay instead would move the deadline every time.
            long fireAt = Instant.now().plus(duration).toEpochMilli();
            pending.add(new NewEvent(EventType.TIMER_STARTED, Payload.encode(fireAt, "")));
            throw new Suspend();
        }
        if (recorded.type() != EventType.TIMER_STARTED) {
            throw new NonDeterministicException(commandIndex - 1, recorded.type().name(), "sleep " + duration);
        }
        if (firedAt(recorded.seq()) == null) {
            throw new Suspend();
        }
    }

    /** When the given timer fired, or null if it's still pending. */
    private Event firedAt(long timerSeq) {
        String reference = Long.toString(timerSeq);
        for (Event event : history) {
            if (event.type() == EventType.TIMER_FIRED && reference.equals(Payload.head(event.payload()))) {
                return event;
            }
        }
        return null;
    }

    /**
     * Registers a handler for a named signal. Handlers are called during replay, in the
     * order the signals were recorded, so whatever state they build up is rebuilt the
     * same way every time.
     */
    public void onSignal(String name, Consumer<String> handler) {
        signalHandlers.put(name, handler);
    }

    /**
     * Waits up to {@code timeout} for {@code condition} to become true, where the
     * condition is normally something the signal handlers set.
     *
     * <p>Returns true if the condition won and false if the timeout did. That single
     * distinction covers both halves of a journey rule: "did B within X minutes" is the
     * true branch, "did not do C within Y minutes" is the false one.
     */
    public boolean awaitUntil(Duration timeout, BooleanSupplier condition) {
        Event timer = nextCommand();

        if (timer == null) {
            // Start the timer before looking at signals, even if one has already arrived.
            // Doing it in this order costs an extra decision round when the condition is
            // met straight away, and in exchange there's exactly one path through here,
            // so a cancelled timer can never be left behind in the index.
            long fireAt = Instant.now().plus(timeout).toEpochMilli();
            pending.add(new NewEvent(EventType.TIMER_STARTED, Payload.encode(fireAt, "")));
            throw new Suspend();
        }
        if (timer.type() != EventType.TIMER_STARTED) {
            throw new NonDeterministicException(
                    commandIndex - 1, timer.type().name(), "awaitUntil " + timeout);
        }

        if (dispatchSignals(condition)) {
            // Cancelling is itself a decision, so it takes a command slot like any other.
            Event cancel = nextCommand();
            if (cancel == null) {
                pending.add(new NewEvent(EventType.TIMER_CANCELLED, Payload.encode(timer.seq(), "")));
            } else if (cancel.type() != EventType.TIMER_CANCELLED) {
                throw new NonDeterministicException(
                        commandIndex - 1, cancel.type().name(), "cancel of timer " + timer.seq());
            }
            return true;
        }
        if (firedAt(timer.seq()) != null) {
            return false;
        }
        throw new Suspend();
    }

    /**
     * Feeds any signals we haven't handled yet to their handlers, checking the condition
     * after each one so we can tell which signal satisfied it.
     */
    private boolean dispatchSignals(BooleanSupplier condition) {
        while (signalCursor < signals.size()) {
            Event signal = signals.get(signalCursor++);
            Consumer<String> handler = signalHandlers.get(Payload.head(signal.payload()));
            if (handler != null) {
                handler.accept(Payload.tail(signal.payload()));
            }
            if (condition.getAsBoolean()) {
                return true;
            }
        }
        return condition.getAsBoolean();
    }

    List<NewEvent> pendingCommands() {
        return pending;
    }

    /**
     * True if the history holds commands the code never got round to issuing, which means
     * the code no longer matches the run it's replaying.
     */
    boolean hasUnconsumedCommands() {
        return commandIndex < commands.size();
    }

    private Event nextCommand() {
        Event recorded = commandIndex < commands.size() ? commands.get(commandIndex) : null;
        commandIndex++;
        return recorded;
    }

    private Event resultFor(long scheduledSeq) {
        String reference = Long.toString(scheduledSeq);
        for (Event event : history) {
            boolean isResult = event.type() == EventType.ACTIVITY_COMPLETED
                    || event.type() == EventType.ACTIVITY_FAILED;
            if (isResult && reference.equals(Payload.head(event.payload()))) {
                return event;
            }
        }
        return null;
    }
}

/**
 * Control flow, not an error: thrown to unwind out of workflow code when it asks for
 * something the history can't answer yet. No stack trace, it gets thrown constantly.
 */
class Suspend extends RuntimeException {

    Suspend() {
        super(null, null, false, false);
    }
}
