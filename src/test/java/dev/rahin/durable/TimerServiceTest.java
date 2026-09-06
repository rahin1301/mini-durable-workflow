package dev.rahin.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TimerServiceTest {

    private static final Duration WAIT = Duration.ofMinutes(5);

    @TempDir
    Path dir;

    private Path dbFile;

    @BeforeEach
    void setUp() {
        dbFile = dir.resolve("workflows.db");
    }

    @Test
    void sleepSchedulesATimerAndBlocks() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(WAIT));

            engine.start("wf-1", "Wait", null);

            assertEquals(List.of(EventType.WORKFLOW_STARTED, EventType.TIMER_STARTED), types(store, "wf-1"));
            assertEquals(WorkflowStatus.RUNNING, store.find("wf-1").orElseThrow().status());
            assertEquals(1, store.dueTimers(Instant.now().plus(WAIT)).size());
        }
    }

    @Test
    void aTimerThatIsNotDueDoesNotFire() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            TimerService timers = new TimerService(store, engine);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(WAIT));
            engine.start("wf-1", "Wait", null);

            assertEquals(0, timers.fireDueTimers(Instant.now()));
            assertEquals(WorkflowStatus.RUNNING, store.find("wf-1").orElseThrow().status());
        }
    }

    @Test
    void aDueTimerFiresAndTheWorkflowCarriesOn() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            TimerService timers = new TimerService(store, engine);
            engine.registerWorkflow("Remind", (ctx, input) -> {
                ctx.sleep(WAIT);
                ctx.executeActivity("sendReminder", input);
            });
            engine.registerActivity("sendReminder", cartId -> "reminded " + cartId);
            engine.start("wf-1", "Remind", "cart-9");

            assertEquals(1, timers.fireDueTimers(Instant.now().plus(WAIT)));

            assertEquals(List.of(
                            EventType.WORKFLOW_STARTED,
                            EventType.TIMER_STARTED,
                            EventType.TIMER_FIRED,
                            EventType.ACTIVITY_SCHEDULED,
                            EventType.ACTIVITY_COMPLETED,
                            EventType.WORKFLOW_COMPLETED),
                    types(store, "wf-1"));
            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
        }
    }

    @Test
    void aTimerFiresAfterARestartEvenThoughNothingWasInMemory() {
        AtomicInteger reminders = new AtomicInteger();

        // process 1: start the workflow, it parks on the timer, then we stop
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = remindEngine(store, reminders);
            engine.start("cart-1", "Remind", "cart-1");

            assertEquals(List.of(EventType.WORKFLOW_STARTED, EventType.TIMER_STARTED), types(store, "cart-1"));
            assertEquals(0, reminders.get());
        }

        // process 2: nothing carried over but the file
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = remindEngine(store, reminders);
            TimerService timers = new TimerService(store, engine);

            engine.recover(); // nothing to do yet, the timer hasn't come due
            assertEquals(0, reminders.get());

            assertEquals(1, timers.fireDueTimers(Instant.now().plus(WAIT)));
            assertEquals(1, reminders.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("cart-1").orElseThrow().status());
        }
    }

    @Test
    void aTimerThatCameDueWhileTheProcessWasDownFiresImmediately() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(Duration.ofMillis(1)));
            engine.start("wf-1", "Wait", null);
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(Duration.ofMillis(1)));
            TimerService timers = new TimerService(store, engine);

            // the fire time is already in the past, so a plain poll catches it
            assertEquals(1, timers.fireDueTimers(Instant.now().plusSeconds(1)));
            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
        }
    }

    @Test
    void theRecordedFireTimeIsNotRecomputedOnReplay() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(WAIT));
            engine.start("wf-1", "Wait", null);

            Instant deadline = store.dueTimers(Instant.now().plus(WAIT)).get(0).fireAt();

            // replaying the workflow several more times must not push the deadline out
            engine.decide("wf-1");
            engine.decide("wf-1");

            assertEquals(deadline, store.dueTimers(Instant.now().plus(WAIT)).get(0).fireAt());
            assertEquals(2, store.readHistory("wf-1").size());
        }
    }

    @Test
    void swappingASleepForAnActivityIsCaught() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.sleep(WAIT));
            engine.start("wf-1", "Wait", null);
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Wait", (ctx, input) -> ctx.executeActivity("somethingElse", input));

            assertThrows(NonDeterministicException.class, engine::recover);
        }
    }

    @Test
    void theBackgroundPollerFiresTimersOnItsOwn() throws Exception {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger reminders = new AtomicInteger();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Remind", (ctx, input) -> {
                ctx.sleep(Duration.ofMillis(50));
                ctx.executeActivity("sendReminder", input);
            });
            engine.registerActivity("sendReminder", cartId -> {
                reminders.incrementAndGet();
                return "reminded";
            });
            engine.start("cart-1", "Remind", "cart-1");

            try (TimerService timers = new TimerService(store, engine)) {
                timers.start(Duration.ofMillis(20));

                long deadline = System.currentTimeMillis() + 5_000;
                while (reminders.get() == 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10);
                }
            }

            assertEquals(1, reminders.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("cart-1").orElseThrow().status());
        }
    }

    private static WorkflowEngine remindEngine(EventStore store, AtomicInteger reminders) {
        WorkflowEngine engine = new WorkflowEngine(store);
        engine.registerWorkflow("Remind", (ctx, input) -> {
            ctx.sleep(WAIT);
            ctx.executeActivity("sendReminder", input);
        });
        engine.registerActivity("sendReminder", cartId -> {
            reminders.incrementAndGet();
            return "reminded " + cartId;
        });
        return engine;
    }

    private static List<EventType> types(EventStore store, String workflowId) {
        return store.readHistory(workflowId).stream().map(Event::type).toList();
    }
}
