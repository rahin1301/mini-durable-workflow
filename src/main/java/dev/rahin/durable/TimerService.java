package dev.rahin.durable;

import dev.rahin.durable.EventStore.NewEvent;
import dev.rahin.durable.EventStore.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Fires timers whose time has come.
 *
 * <p>Timers live in the log, so this doesn't hold any state of its own. Starting it after
 * a restart picks up everything that was pending, including timers that came due while the
 * process was down, which just fire straight away.
 */
public class TimerService implements AutoCloseable {

    private final EventStore store;
    private final WorkflowEngine engine;
    private ScheduledExecutorService poller;

    public TimerService(EventStore store, WorkflowEngine engine) {
        this.store = store;
        this.engine = engine;
    }

    /** Polls in the background until closed. */
    public void start(Duration interval) {
        if (poller != null) {
            throw new IllegalStateException("already started");
        }
        poller = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "timer-service");
            thread.setDaemon(true);
            return thread;
        });
        poller.scheduleWithFixedDelay(() -> {
            try {
                fireDueTimers();
            } catch (RuntimeException e) {
                // An uncaught exception here would silently stop the poller for good.
                System.err.println("timer poll failed: " + e);
            }
        }, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    public int fireDueTimers() {
        return fireDueTimers(Instant.now());
    }

    /** Returns how many timers fired. Takes the time so tests don't have to wait. */
    public int fireDueTimers(Instant now) {
        int fired = 0;
        for (Timer timer : store.dueTimers(now)) {
            // Record that it fired before anyone acts on it. Dying here just means the
            // workflow is driven forward by whoever picks the log up next.
            try {
                store.append(
                        timer.workflowId(),
                        store.nextSequence(timer.workflowId()),
                        List.of(new NewEvent(EventType.TIMER_FIRED, Payload.encode(timer.seq(), ""))));
            } catch (ConflictException e) {
                continue; // something else moved this workflow along; it'll come round again
            }
            engine.drive(timer.workflowId());
            fired++;
        }
        return fired;
    }

    @Override
    public void close() {
        if (poller != null) {
            poller.shutdownNow();
            poller = null;
        }
    }
}
