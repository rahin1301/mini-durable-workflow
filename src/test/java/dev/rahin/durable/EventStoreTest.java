package dev.rahin.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.rahin.durable.EventStore.NewEvent;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EventStoreTest {

    @TempDir
    Path dir;

    private Path dbFile;
    private EventStore store;

    @BeforeEach
    void setUp() {
        dbFile = dir.resolve("workflows.db");
        store = new EventStore(dbFile);
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.close();
        }
    }

    @Test
    void newWorkflowStartsWithWorkflowStartedAtZero() {
        WorkflowInstance instance = store.createWorkflow("cart-1", "CartAbandonment", "{\"cartId\":\"c-1\"}");

        assertEquals(WorkflowStatus.RUNNING, instance.status());

        List<Event> history = store.readHistory("cart-1");
        assertEquals(1, history.size());
        assertEquals(EventType.WORKFLOW_STARTED, history.get(0).type());
        assertEquals(0, history.get(0).seq());
        assertEquals("{\"cartId\":\"c-1\"}", history.get(0).payload());
        assertEquals(1, store.nextSequence("cart-1"));
    }

    @Test
    void startingTheSameWorkflowTwiceIsRejected() {
        store.createWorkflow("cart-1", "CartAbandonment", null);

        assertThrows(WorkflowExistsException.class,
                () -> store.createWorkflow("cart-1", "CartAbandonment", null));
        assertEquals(1, store.readHistory("cart-1").size());
    }

    @Test
    void appendedEventsKeepTheirOrder() {
        store.createWorkflow("cart-1", "CartAbandonment", null);

        assertEquals(3, store.append("cart-1", 1, List.of(
                new NewEvent(EventType.SIGNAL_RECEIVED, "checkout"),
                new NewEvent(EventType.ACTIVITY_SCHEDULED, "sendReminder"))));
        assertEquals(4, store.append("cart-1", 3, List.of(
                new NewEvent(EventType.ACTIVITY_SCHEDULED, "sendReminder"))));

        List<Event> history = store.readHistory("cart-1");
        assertEquals(List.of(
                        EventType.WORKFLOW_STARTED,
                        EventType.SIGNAL_RECEIVED,
                        EventType.ACTIVITY_SCHEDULED,
                        EventType.ACTIVITY_SCHEDULED),
                history.stream().map(Event::type).toList());
        assertEquals(List.of(0L, 1L, 2L, 3L), history.stream().map(Event::seq).toList());
    }

    @Test
    void historyCanBeReadFromASequence() {
        store.createWorkflow("cart-1", "CartAbandonment", null);
        store.append("cart-1", 1, List.of(
                new NewEvent(EventType.SIGNAL_RECEIVED, "checkout"),
                new NewEvent(EventType.ACTIVITY_SCHEDULED, "sendReminder")));

        List<Event> tail = store.readHistory("cart-1", 2);

        assertEquals(1, tail.size());
        assertEquals(EventType.ACTIVITY_SCHEDULED, tail.get(0).type());
    }

    @Test
    void staleAppendIsRejectedAndWritesNothing() {
        store.createWorkflow("cart-1", "CartAbandonment", null);
        store.append("cart-1", 1, List.of(new NewEvent(EventType.SIGNAL_RECEIVED, "checkout")));

        assertThrows(ConflictException.class,
                () -> store.append("cart-1", 1, List.of(new NewEvent(EventType.ACTIVITY_SCHEDULED, "x"))));
        assertEquals(2, store.readHistory("cart-1").size());
    }

    @Test
    void sequencesAreScopedPerWorkflow() {
        store.createWorkflow("cart-1", "CartAbandonment", null);
        store.createWorkflow("cart-2", "CartAbandonment", null);

        store.append("cart-1", 1, List.of(new NewEvent(EventType.SIGNAL_RECEIVED, "checkout")));

        assertEquals(2, store.nextSequence("cart-1"));
        assertEquals(1, store.nextSequence("cart-2"));
    }

    @Test
    void terminalEventClosesTheHistory() {
        store.createWorkflow("cart-1", "CartAbandonment", null);
        store.append("cart-1", 1, List.of(NewEvent.of(EventType.WORKFLOW_COMPLETED)), WorkflowStatus.COMPLETED);

        assertThrows(StoreException.class,
                () -> store.append("cart-1", 2, List.of(new NewEvent(EventType.SIGNAL_RECEIVED, "checkout"))));
    }

    @Test
    void completingAWorkflowUpdatesItsStatus() {
        store.createWorkflow("cart-1", "CartAbandonment", null);
        assertEquals(List.of("cart-1"), store.idsWithStatus(WorkflowStatus.RUNNING));

        store.append("cart-1", 1, List.of(NewEvent.of(EventType.WORKFLOW_COMPLETED)), WorkflowStatus.COMPLETED);

        assertEquals(List.of(), store.idsWithStatus(WorkflowStatus.RUNNING));
        assertEquals(WorkflowStatus.COMPLETED, store.find("cart-1").orElseThrow().status());
    }

    @Test
    void unknownWorkflowIsRejected() {
        assertThrows(StoreException.class, () -> store.nextSequence("nope"));
        assertThrows(StoreException.class,
                () -> store.append("nope", 0, List.of(NewEvent.of(EventType.SIGNAL_RECEIVED))));
        assertEquals(Optional.empty(), store.find("nope"));
        assertEquals(List.of(), store.readHistory("nope"));
    }

    @Test
    void historySurvivesReopeningTheFile() {
        store.createWorkflow("cart-1", "CartAbandonment", "{\"cartId\":\"c-1\"}");
        store.append("cart-1", 1, List.of(
                new NewEvent(EventType.SIGNAL_RECEIVED, "checkout"),
                new NewEvent(EventType.ACTIVITY_SCHEDULED, "sendReminder")));
        store.close();

        // same thing a restarted process sees
        try (EventStore reopened = new EventStore(dbFile)) {
            assertEquals(3, reopened.readHistory("cart-1").size());
            assertEquals(3, reopened.nextSequence("cart-1"));
            assertEquals(List.of("cart-1"), reopened.idsWithStatus(WorkflowStatus.RUNNING));
            assertEquals("{\"cartId\":\"c-1\"}", reopened.find("cart-1").orElseThrow().input());
        }
        store = null;
    }

    @Test
    void onlyOneAppendCanWinASequence() throws Exception {
        store.createWorkflow("cart-1", "CartAbandonment", null);

        int writers = 8;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger won = new AtomicInteger();
        AtomicInteger conflicted = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(writers)) {
            for (int i = 0; i < writers; i++) {
                String payload = "writer-" + i;
                futures.add(pool.submit(() -> {
                    start.await();
                    try {
                        store.append("cart-1", 1, List.of(new NewEvent(EventType.SIGNAL_RECEIVED, payload)));
                        won.incrementAndGet();
                    } catch (ConflictException expected) {
                        conflicted.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }

        assertEquals(1, won.get());
        assertEquals(writers - 1, conflicted.get());
        assertEquals(2, store.readHistory("cart-1").size());
        assertEquals(List.of(0L, 1L), store.readHistory("cart-1").stream().map(Event::seq).toList());
    }
}
