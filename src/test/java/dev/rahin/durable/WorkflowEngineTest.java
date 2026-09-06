package dev.rahin.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowEngineTest {

    @TempDir
    Path dir;

    private Path dbFile;

    @BeforeEach
    void setUp() {
        dbFile = dir.resolve("workflows.db");
    }

    @Test
    void workflowRunsAnActivityAndCompletes() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            engine.registerActivity("greet", name -> "hello " + name);

            engine.start("wf-1", "Greet", "rahin");

            assertEquals(List.of(
                            EventType.WORKFLOW_STARTED,
                            EventType.ACTIVITY_SCHEDULED,
                            EventType.ACTIVITY_COMPLETED,
                            EventType.WORKFLOW_COMPLETED),
                    types(store, "wf-1"));
            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
        }
    }

    @Test
    void activityResultIsVisibleToTheRestOfTheWorkflow() {
        try (EventStore store = new EventStore(dbFile)) {
            List<String> seen = new ArrayList<>();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Chain", (ctx, input) -> {
                String first = ctx.executeActivity("upper", input);
                String second = ctx.executeActivity("exclaim", first);
                seen.add(second);
            });
            engine.registerActivity("upper", String::toUpperCase);
            engine.registerActivity("exclaim", s -> s + "!");

            engine.start("wf-1", "Chain", "hi");

            assertEquals(List.of("HI!"), seen);
        }
    }

    @Test
    void workflowCodeRerunsButEachActivityRunsOnce() {
        try (EventStore store = new EventStore(dbFile)) {
            AtomicInteger workflowPasses = new AtomicInteger();
            AtomicInteger activityCalls = new AtomicInteger();

            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Three", (ctx, input) -> {
                workflowPasses.incrementAndGet();
                ctx.executeActivity("step", "a");
                ctx.executeActivity("step", "b");
                ctx.executeActivity("step", "c");
            });
            engine.registerActivity("step", input -> {
                activityCalls.incrementAndGet();
                return input;
            });

            engine.start("wf-1", "Three", null);

            // Four passes: one per activity that had to be scheduled, plus the final one
            // that gets all the way through and completes.
            assertEquals(4, workflowPasses.get());
            assertEquals(3, activityCalls.get());
        }
    }

    @Test
    void isReplayingIsFalseOnlyOnTheLiveEdge() {
        try (EventStore store = new EventStore(dbFile)) {
            List<Boolean> flags = new ArrayList<>();
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Two", (ctx, input) -> {
                flags.add(ctx.isReplaying());
                ctx.executeActivity("step", "a");
                flags.add(ctx.isReplaying());
                ctx.executeActivity("step", "b");
            });
            engine.registerActivity("step", input -> input);

            engine.start("wf-1", "Two", null);

            // pass 1 starts live; passes 2 and 3 start by re-deriving recorded commands
            assertEquals(List.of(false, true, false, true, true), flags);
        }
    }

    @Test
    void aWorkflowInterruptedMidActivityResumesOnRestart() {
        AtomicInteger activityCalls = new AtomicInteger();

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = engineFor(store, activityCalls);
            store.createWorkflow("wf-1", "Greet", "rahin");
            engine.decide("wf-1"); // schedules the activity, then the process "dies"

            assertEquals(List.of(EventType.WORKFLOW_STARTED, EventType.ACTIVITY_SCHEDULED), types(store, "wf-1"));
            assertEquals(0, activityCalls.get());
        }

        // fresh process: new store, new engine, nothing carried over but the file
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = engineFor(store, activityCalls);
            engine.recover();

            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
            assertEquals(1, activityCalls.get());
        }
    }

    @Test
    void anActivityThatRanButWasNotRecordedRunsAgain() {
        AtomicInteger activityCalls = new AtomicInteger();
        // Throws an Error, which the engine deliberately doesn't catch, so the side effect
        // has happened but no result was recorded. Same shape as being killed mid-activity.
        Activity flaky = input -> {
            if (activityCalls.incrementAndGet() == 1) {
                throw new StackOverflowError("process died after the side effect");
            }
            return "ok";
        };

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            engine.registerActivity("greet", flaky);

            assertThrows(StackOverflowError.class, () -> engine.start("wf-1", "Greet", "rahin"));
            assertEquals(List.of(EventType.WORKFLOW_STARTED, EventType.ACTIVITY_SCHEDULED), types(store, "wf-1"));
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            engine.registerActivity("greet", flaky);
            engine.recover();

            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
            assertEquals(2, activityCalls.get(), "at-least-once: the side effect happened twice");
        }
    }

    @Test
    void changingWorkflowCodeUnderARunningWorkflowIsCaught() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            store.createWorkflow("wf-1", "Greet", "rahin");
            engine.decide("wf-1");
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            // same workflow type, different code: it now calls a different activity
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("shout", input));
            engine.registerActivity("shout", String::toUpperCase);

            NonDeterministicException e =
                    assertThrows(NonDeterministicException.class, engine::recover);
            assertTrue(e.getMessage().contains("activity greet"));
            assertTrue(e.getMessage().contains("activity shout"));
        }
    }

    @Test
    void aWorkflowThatFinishesEarlyAfterACodeChangeIsCaught() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            store.createWorkflow("wf-1", "Greet", "rahin");
            engine.decide("wf-1");
        }

        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> { /* activity call removed */ });

            assertThrows(NonDeterministicException.class, engine::recover);
        }
    }

    @Test
    void aFailingActivityIsThrownIntoTheWorkflow() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
            engine.registerActivity("greet", input -> {
                throw new IllegalStateException("downstream is down");
            });

            engine.start("wf-1", "Greet", "rahin");

            assertEquals(List.of(
                            EventType.WORKFLOW_STARTED,
                            EventType.ACTIVITY_SCHEDULED,
                            EventType.ACTIVITY_FAILED,
                            EventType.WORKFLOW_FAILED),
                    types(store, "wf-1"));
            assertEquals(WorkflowStatus.FAILED, store.find("wf-1").orElseThrow().status());
        }
    }

    @Test
    void aWorkflowCanHandleAFailedActivityAndCarryOn() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Fallback", (ctx, input) -> {
                try {
                    ctx.executeActivity("primary", input);
                } catch (ActivityFailedException e) {
                    ctx.executeActivity("backup", input);
                }
            });
            engine.registerActivity("primary", input -> {
                throw new IllegalStateException("nope");
            });
            engine.registerActivity("backup", input -> "sent by fallback");

            engine.start("wf-1", "Fallback", "rahin");

            assertEquals(WorkflowStatus.COMPLETED, store.find("wf-1").orElseThrow().status());
            assertEquals(List.of(
                            EventType.WORKFLOW_STARTED,
                            EventType.ACTIVITY_SCHEDULED,
                            EventType.ACTIVITY_FAILED,
                            EventType.ACTIVITY_SCHEDULED,
                            EventType.ACTIVITY_COMPLETED,
                            EventType.WORKFLOW_COMPLETED),
                    types(store, "wf-1"));
        }
    }

    @Test
    void anUnregisteredActivityFailsTheWorkflowRatherThanHanging() {
        try (EventStore store = new EventStore(dbFile)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("missing", input));

            engine.start("wf-1", "Greet", "rahin");

            assertEquals(WorkflowStatus.FAILED, store.find("wf-1").orElseThrow().status());
        }
    }

    private static WorkflowEngine engineFor(EventStore store, AtomicInteger activityCalls) {
        WorkflowEngine engine = new WorkflowEngine(store);
        engine.registerWorkflow("Greet", (ctx, input) -> ctx.executeActivity("greet", input));
        engine.registerActivity("greet", name -> {
            activityCalls.incrementAndGet();
            return "hello " + name;
        });
        return engine;
    }

    private static List<EventType> types(EventStore store, String workflowId) {
        return store.readHistory(workflowId).stream().map(Event::type).toList();
    }
}
