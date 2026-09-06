package dev.rahin.durable;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

/**
 * Append-only event log, backed by SQLite. This is the engine's only source of truth:
 * a workflow's progress lives nowhere else, so recovering one after a crash just means
 * reading its history back and replaying it.
 *
 * <p>Writes are optimistic. You read a history, work out what happens next, and append
 * at the sequence you expect to be free. If something else got there first you get a
 * {@link ConflictException} and nothing is written.
 */
public class EventStore implements AutoCloseable {

    private static final String[] SCHEMA = {
        """
        CREATE TABLE IF NOT EXISTS workflow_instances (
            id            TEXT PRIMARY KEY,
            workflow_type TEXT NOT NULL,
            input         TEXT,
            status        TEXT NOT NULL,
            created_at    INTEGER NOT NULL,
            updated_at    INTEGER NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances (status)",
        """
        CREATE TABLE IF NOT EXISTS timers (
            workflow_id TEXT NOT NULL,
            seq         INTEGER NOT NULL,
            fire_at     INTEGER NOT NULL,
            fired       INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (workflow_id, seq)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_timers_due ON timers (fired, fire_at)",
        """
        CREATE TABLE IF NOT EXISTS events (
            workflow_id TEXT NOT NULL REFERENCES workflow_instances (id),
            seq         INTEGER NOT NULL,
            type        TEXT NOT NULL,
            payload     TEXT,
            recorded_at INTEGER NOT NULL,
            PRIMARY KEY (workflow_id, seq)
        )
        """
    };

    private final SQLiteDataSource dataSource;

    public EventStore(Path file) {
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        // FULL fsyncs on commit. WAL + NORMAL survives the process being killed but can
        // lose the last few commits if the machine loses power, which isn't good enough
        // for something whose whole job is not losing progress.
        config.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
        // Take the write lock at BEGIN rather than on first write. Deferred transactions
        // that upgrade halfway through are how you get SQLITE_BUSY between the timer
        // service and the decider.
        config.setTransactionMode(SQLiteConfig.TransactionMode.IMMEDIATE);
        config.enforceForeignKeys(true);
        config.setBusyTimeout(5_000);

        dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + file.toAbsolutePath());

        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            for (String ddl : SCHEMA) {
                stmt.execute(ddl);
            }
        } catch (SQLException e) {
            throw new StoreException("could not open event store at " + file, e);
        }
    }

    /** A timer that hasn't fired yet. {@code seq} is where its TIMER_STARTED landed. */
    public record Timer(String workflowId, long seq, java.time.Instant fireAt) {
    }

    /** An event on its way in. Sequence and timestamp are the store's to assign. */
    public record NewEvent(EventType type, String payload) {

        public static NewEvent of(EventType type) {
            return new NewEvent(type, null);
        }
    }

    /**
     * Creates a workflow and writes its WORKFLOW_STARTED event at seq 0 in the same
     * transaction, so every history starts the same way.
     */
    public WorkflowInstance createWorkflow(String workflowId, String workflowType, String input) {
        Instant now = Instant.now();
        return tx(conn -> {
            if (exists(conn, workflowId)) {
                throw new WorkflowExistsException(workflowId);
            }
            try (PreparedStatement stmt = conn.prepareStatement("""
                    INSERT INTO workflow_instances (id, workflow_type, input, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """)) {
                stmt.setString(1, workflowId);
                stmt.setString(2, workflowType);
                stmt.setString(3, input);
                stmt.setString(4, WorkflowStatus.RUNNING.name());
                stmt.setLong(5, now.toEpochMilli());
                stmt.setLong(6, now.toEpochMilli());
                stmt.executeUpdate();
            }
            insert(conn, workflowId, 0, List.of(new NewEvent(EventType.WORKFLOW_STARTED, input)), now);
            return new WorkflowInstance(workflowId, workflowType, input, WorkflowStatus.RUNNING, now, now);
        });
    }

    /** Appends events atomically at {@code expectedSeq}. Returns the next free sequence. */
    public long append(String workflowId, long expectedSeq, List<NewEvent> events) {
        return append(workflowId, expectedSeq, events, null);
    }

    /**
     * Appends events and moves the workflow to {@code newStatus} in one transaction.
     * Used to close a workflow: if the terminal event and the status didn't land together
     * recovery could find a workflow whose history says it finished but whose status
     * still says running.
     */
    public long append(String workflowId, long expectedSeq, List<NewEvent> events, WorkflowStatus newStatus) {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("nothing to append");
        }
        Instant now = Instant.now();

        return tx(conn -> {
            Tail tail = lastEvent(conn, workflowId);
            if (tail == null) {
                throw new StoreException("no such workflow: " + workflowId);
            }
            if (tail.type().isTerminal()) {
                throw new StoreException(
                        workflowId + " is closed by " + tail.type() + " and takes no more events");
            }

            long nextSeq = tail.seq() + 1;
            if (nextSeq != expectedSeq) {
                throw new ConflictException(workflowId, expectedSeq, nextSeq);
            }

            insert(conn, workflowId, nextSeq, events, now);
            try (PreparedStatement stmt = conn.prepareStatement(
                    "UPDATE workflow_instances SET status = coalesce(?, status), updated_at = ? WHERE id = ?")) {
                stmt.setString(1, newStatus == null ? null : newStatus.name());
                stmt.setLong(2, now.toEpochMilli());
                stmt.setString(3, workflowId);
                stmt.executeUpdate();
            }
            return nextSeq + events.size();
        });
    }

    public List<Event> readHistory(String workflowId) {
        return readHistory(workflowId, 0);
    }

    public List<Event> readHistory(String workflowId, long fromSeq) {
        return query(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement("""
                    SELECT seq, type, payload, recorded_at FROM events
                    WHERE workflow_id = ? AND seq >= ? ORDER BY seq
                    """)) {
                stmt.setString(1, workflowId);
                stmt.setLong(2, fromSeq);
                try (ResultSet rs = stmt.executeQuery()) {
                    List<Event> history = new ArrayList<>();
                    while (rs.next()) {
                        history.add(new Event(
                                workflowId,
                                rs.getLong("seq"),
                                EventType.valueOf(rs.getString("type")),
                                rs.getString("payload"),
                                Instant.ofEpochMilli(rs.getLong("recorded_at"))));
                    }
                    return history;
                }
            }
        });
    }

    /** The sequence the next appended event will take. */
    public long nextSequence(String workflowId) {
        return query(conn -> {
            Tail tail = lastEvent(conn, workflowId);
            if (tail == null) {
                throw new StoreException("no such workflow: " + workflowId);
            }
            return tail.seq() + 1;
        });
    }

    public Optional<WorkflowInstance> find(String workflowId) {
        return query(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT * FROM workflow_instances WHERE id = ?")) {
                stmt.setString(1, workflowId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(new WorkflowInstance(
                            rs.getString("id"),
                            rs.getString("workflow_type"),
                            rs.getString("input"),
                            WorkflowStatus.valueOf(rs.getString("status")),
                            Instant.ofEpochMilli(rs.getLong("created_at")),
                            Instant.ofEpochMilli(rs.getLong("updated_at"))));
                }
            }
        });
    }

    /** Recovery calls this with RUNNING on startup to find what was in flight. */
    public List<String> idsWithStatus(WorkflowStatus status) {
        return query(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT id FROM workflow_instances WHERE status = ? ORDER BY created_at")) {
                stmt.setString(1, status.name());
                try (ResultSet rs = stmt.executeQuery()) {
                    List<String> ids = new ArrayList<>();
                    while (rs.next()) {
                        ids.add(rs.getString("id"));
                    }
                    return ids;
                }
            }
        });
    }

    /**
     * Timers that should have fired by {@code now}. Read from the index rather than by
     * scanning histories, but it's only an index: every row here is implied by a
     * TIMER_STARTED event that hasn't been answered yet.
     */
    public List<Timer> dueTimers(Instant now) {
        return query(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT workflow_id, seq, fire_at FROM timers WHERE fired = 0 AND fire_at <= ? ORDER BY fire_at")) {
                stmt.setLong(1, now.toEpochMilli());
                try (ResultSet rs = stmt.executeQuery()) {
                    List<Timer> due = new ArrayList<>();
                    while (rs.next()) {
                        due.add(new Timer(
                                rs.getString("workflow_id"),
                                rs.getLong("seq"),
                                Instant.ofEpochMilli(rs.getLong("fire_at"))));
                    }
                    return due;
                }
            }
        });
    }

    @Override
    public void close() {
        // SQLiteDataSource doesn't pool, connections are closed at each use site.
    }

    // --- internals ---

    @FunctionalInterface
    private interface SqlWork<T> {
        T run(Connection conn) throws SQLException;
    }

    private <T> T tx(SqlWork<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                T result = work.run(conn);
                conn.commit();
                return result;
            } catch (Throwable t) {
                conn.rollback();
                throw t;
            }
        } catch (SQLException e) {
            throw new StoreException("transaction failed", e);
        }
    }

    private <T> T query(SqlWork<T> work) {
        try (Connection conn = dataSource.getConnection()) {
            return work.run(conn);
        } catch (SQLException e) {
            throw new StoreException("query failed", e);
        }
    }

    private record Tail(long seq, EventType type) {
    }

    /** Last event of a history, or null if the workflow doesn't exist. */
    private static Tail lastEvent(Connection conn, String workflowId) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT seq, type FROM events WHERE workflow_id = ? ORDER BY seq DESC LIMIT 1")) {
            stmt.setString(1, workflowId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new Tail(rs.getLong("seq"), EventType.valueOf(rs.getString("type")));
            }
        }
    }

    private static boolean exists(Connection conn, String workflowId) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT 1 FROM workflow_instances WHERE id = ?")) {
            stmt.setString(1, workflowId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void insert(Connection conn, String workflowId, long firstSeq, List<NewEvent> events, Instant now)
            throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement("""
                INSERT INTO events (workflow_id, seq, type, payload, recorded_at) VALUES (?, ?, ?, ?, ?)
                """)) {
            long seq = firstSeq;
            for (NewEvent event : events) {
                stmt.setString(1, workflowId);
                stmt.setLong(2, seq++);
                stmt.setString(3, event.type().name());
                stmt.setString(4, event.payload());
                stmt.setLong(5, now.toEpochMilli());
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
        indexTimers(conn, workflowId, firstSeq, events);
    }

    /**
     * Keeps the timers table in step with the events being written, in the same
     * transaction. It's a derived index: a timer exists because a TIMER_STARTED event
     * exists, and this table only saves us scanning every history to find what's due.
     */
    private static void indexTimers(Connection conn, String workflowId, long firstSeq, List<NewEvent> events)
            throws SQLException {
        long seq = firstSeq;
        for (NewEvent event : events) {
            long eventSeq = seq++;
            switch (event.type()) {
                case TIMER_STARTED -> {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "INSERT INTO timers (workflow_id, seq, fire_at) VALUES (?, ?, ?)")) {
                        stmt.setString(1, workflowId);
                        stmt.setLong(2, eventSeq);
                        stmt.setLong(3, Long.parseLong(Payload.head(event.payload())));
                        stmt.executeUpdate();
                    }
                }
                case TIMER_FIRED, TIMER_CANCELLED -> {
                    try (PreparedStatement stmt = conn.prepareStatement(
                            "UPDATE timers SET fired = 1 WHERE workflow_id = ? AND seq = ?")) {
                        stmt.setString(1, workflowId);
                        stmt.setLong(2, Long.parseLong(Payload.head(event.payload())));
                        stmt.executeUpdate();
                    }
                }
                default -> {
                    // nothing else touches the timer index
                }
            }
        }
    }
}
