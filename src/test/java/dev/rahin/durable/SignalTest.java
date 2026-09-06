package dev.rahin.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SignalTest {

    private static final Duration WINDOW = Duration.ofMinutes(5);

    @TempDir
    Path dir;

    private Path dbFile;

    @BeforeEach
    void setUp() {
        dbFile = dir.resolve("workflows.db");
    }

    @Test
    void aSignalThatSatisfiesTheConditionCancelsTheTimer() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = engineWith(store, campaigns);
            engine.start("user-1", "Inaction", "user-1");

            assertTrue(engine.signal("user-1", "CHECKOUT", null));

            assertEquals(List.of(
                            EventType.WORKFLOW_STARTED,
                            EventType.TIMER_STARTED,
                            EventType.SIGNAL_RECEIVED,
                            EventType.TIMER_CANCELLED,
                            EventType.WORKFLOW_COMPLETED),
                    types(store, "user-1"));
            assertEquals(0, campaigns.get(), "the user acted, so no campaign");
            assertEquals(List.of(), store.dueTimers(Instant.now().plus(WINDOW)));
        }
    }

    @Test
    void theTimerWinsWhenNoSignalArrives() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = engineWith(store, campaigns);
            TimerService timers = new TimerService(store, engine);
            engine.start("user-1", "Inaction", "user-1");

            assertEquals(1, timers.fireDueTimers(Instant.now().plus(WINDOW)));

            assertEquals(1, campaigns.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
        }
    }

    @Test
    void aSignalThatArrivedBeforeTheWaitStartedStillCounts() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerActivity("sendCampaign", user -> {
                campaigns.incrementAndGet();
                return "sent";
            });
            engine.registerWorkflow("Inaction", (ctx, user) -> {
                // an activity first, so a signal can land before the wait begins
                ctx.executeActivity("warmUp", user);
                Set<String> seen = new HashSet<>();
                ctx.onSignal("CHECKOUT", p -> seen.add("CHECKOUT"));
                if (!ctx.awaitUntil(WINDOW, () -> seen.contains("CHECKOUT"))) {
                    ctx.executeActivity("sendCampaign", user);
                }
            });
            engine.registerActivity("warmUp", user -> "ok");

            store.createWorkflow("user-1", "Inaction", "user-1");
            engine.decide("user-1"); // schedules warmUp and stops there
            store.append("user-1", store.nextSequence("user-1"),
                    List.of(new EventStore.NewEvent(EventType.SIGNAL_RECEIVED, "CHECKOUT\n")));

            engine.drive("user-1");

            assertEquals(0, campaigns.get(), "the buffered signal should still be seen");
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
        }
    }

    @Test
    void aSignalDeliveredAfterARestartIsHonoured() {
        AtomicInteger campaigns = new AtomicInteger();

        try (EventStore store = new EventStore(dbFile)) {
            engineWith(store, campaigns).start("user-1", "Inaction", "user-1");
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = engineWith(store, campaigns);
            engine.recover();

            assertTrue(engine.signal("user-1", "CHECKOUT", null));
            assertEquals(0, campaigns.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
        }
    }

    @Test
    void signallingAFinishedWorkflowIsRejectedRatherThanThrowing() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = engineWith(store, campaigns);
            engine.start("user-1", "Inaction", "user-1");
            engine.signal("user-1", "CHECKOUT", null);

            assertFalse(engine.signal("user-1", "CHECKOUT", null));
        }
    }

    @Test
    void replayingACancelledTimerDoesNotCancelItTwice() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = engineWith(store, campaigns);
            engine.start("user-1", "Inaction", "user-1");
            engine.signal("user-1", "CHECKOUT", null);

            List<EventType> afterFirstRun = types(store, "user-1");

            // the workflow is closed, so replaying it must be a no-op rather than
            // appending a second TIMER_CANCELLED
            assertFalse(engine.decide("user-1"));
            assertEquals(afterFirstRun, types(store, "user-1"));
        }
    }

    @Test
    void aTwoWindowJourneyRuns() {
        // did A (start), then B within window one, and NOT C within window two
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerActivity("sendCampaign", user -> {
                campaigns.incrementAndGet();
                return "sent";
            });
            engine.registerWorkflow("Journey", (ctx, user) -> {
                Set<String> seen = new HashSet<>();
                ctx.onSignal("PRODUCT_VIEWED", p -> seen.add("PRODUCT_VIEWED"));
                ctx.onSignal("CHECKOUT", p -> seen.add("CHECKOUT"));

                if (!ctx.awaitUntil(WINDOW, () -> seen.contains("PRODUCT_VIEWED"))) {
                    return; // never got to the product, not our audience
                }
                if (ctx.awaitUntil(WINDOW, () -> seen.contains("CHECKOUT"))) {
                    return; // they bought, leave them alone
                }
                ctx.executeActivity("sendCampaign", user);
            });
            TimerService timers = new TimerService(store, engine);

            engine.start("user-1", "Journey", "user-1");
            engine.signal("user-1", "PRODUCT_VIEWED", null);

            assertEquals(0, campaigns.get(), "still inside the second window");

            timers.fireDueTimers(Instant.now().plus(WINDOW).plusSeconds(1));

            assertEquals(1, campaigns.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
        }
    }

    @Test
    void theSameJourneyStopsWhenTheUserChecksOut() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger campaigns = new AtomicInteger();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerActivity("sendCampaign", user -> {
                campaigns.incrementAndGet();
                return "sent";
            });
            engine.registerWorkflow("Journey", (ctx, user) -> {
                Set<String> seen = new HashSet<>();
                ctx.onSignal("PRODUCT_VIEWED", p -> seen.add("PRODUCT_VIEWED"));
                ctx.onSignal("CHECKOUT", p -> seen.add("CHECKOUT"));
                if (!ctx.awaitUntil(WINDOW, () -> seen.contains("PRODUCT_VIEWED"))) {
                    return;
                }
                if (ctx.awaitUntil(WINDOW, () -> seen.contains("CHECKOUT"))) {
                    return;
                }
                ctx.executeActivity("sendCampaign", user);
            });

            engine.start("user-1", "Journey", "user-1");
            engine.signal("user-1", "PRODUCT_VIEWED", null);
            engine.signal("user-1", "CHECKOUT", null);

            assertEquals(0, campaigns.get());
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
            assertEquals(List.of(), store.dueTimers(Instant.now().plus(WINDOW).plusSeconds(1)));
        }
    }

    @Test
    void anUndeliveredMessageFallsBackToTheNextChannel() {
        try (EventStore store = new EventStore(dbFile)) {
            List<String> sent = new ArrayList<>();
            WorkflowEngine engine = engagementEngine(store, sent);
            TimerService timers = new TimerService(store, engine);

            engine.start("user-1", "Engagement", "user-1");
            assertEquals(List.of("push"), sent);

            // no delivery confirmation arrives before the window closes
            timers.fireDueTimers(Instant.now().plus(WINDOW).plusSeconds(1));

            assertEquals(List.of("push", "sms"), sent);
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
        }
    }

    @Test
    void aDeliveredMessageEndsTheChain() {
        try (EventStore store = new EventStore(dbFile)) {
            List<String> sent = new ArrayList<>();
            WorkflowEngine engine = engagementEngine(store, sent);
            TimerService timers = new TimerService(store, engine);

            engine.start("user-1", "Engagement", "user-1");
            engine.signal("user-1", "DELIVERY_UPDATE", "VENDOR_DELIVERED");

            assertEquals(List.of("push"), sent, "no fallback once it's confirmed delivered");
            assertEquals(WorkflowStatus.COMPLETED, store.find("user-1").orElseThrow().status());
            assertEquals(0, timers.fireDueTimers(Instant.now().plus(WINDOW).plusSeconds(1)));
        }
    }

    @Test
    void aDeliveryUpdateThatIsNotDeliveredStillFallsBack() {
        try (EventStore store = new EventStore(dbFile)) {
            List<String> sent = new ArrayList<>();
            WorkflowEngine engine = engagementEngine(store, sent);
            TimerService timers = new TimerService(store, engine);

            engine.start("user-1", "Engagement", "user-1");
            engine.signal("user-1", "DELIVERY_UPDATE", "VENDOR_UNDELIVERED");

            assertEquals(List.of("push"), sent, "still inside the window, nothing decided yet");

            timers.fireDueTimers(Instant.now().plus(WINDOW).plusSeconds(1));

            assertEquals(List.of("push", "sms"), sent);
        }
    }

    /** send a push, and fall back to SMS unless it's confirmed delivered in time */
    private static WorkflowEngine engagementEngine(EventStore store, List<String> sent) {
        WorkflowEngine engine = new WorkflowEngine(store);
        engine.registerWorkflow("Engagement", (ctx, user) -> {
            AtomicReference<String> delivery = new AtomicReference<>();
            ctx.onSignal("DELIVERY_UPDATE", delivery::set);

            ctx.executeActivity("sendPush", user);
            if (ctx.awaitUntil(WINDOW, () -> "VENDOR_DELIVERED".equals(delivery.get()))) {
                return;
            }
            ctx.executeActivity("sendSms", user);
        });
        engine.registerActivity("sendPush", user -> {
            sent.add("push");
            return "VENDOR_SENT";
        });
        engine.registerActivity("sendSms", user -> {
            sent.add("sms");
            return "VENDOR_SENT";
        });
        return engine;
    }

    private static WorkflowEngine engineWith(EventStore store, AtomicInteger campaigns) {
        WorkflowEngine engine = new WorkflowEngine(store);
        engine.registerWorkflow("Inaction", (ctx, user) -> {
            Set<String> seen = new HashSet<>();
            ctx.onSignal("CHECKOUT", p -> seen.add("CHECKOUT"));
            if (!ctx.awaitUntil(WINDOW, () -> seen.contains("CHECKOUT"))) {
                ctx.executeActivity("sendCampaign", user);
            }
        });
        engine.registerActivity("sendCampaign", user -> {
            campaigns.incrementAndGet();
            return "sent";
        });
        return engine;
    }

    private static List<EventType> types(EventStore store, String workflowId) {
        return store.readHistory(workflowId).stream().map(Event::type).toList();
    }
}
