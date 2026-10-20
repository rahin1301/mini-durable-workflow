# Mini Durable Workflow

A small, from-scratch version of the core idea behind Temporal: workflows that can wait on
timers and events, get killed halfway through, and continue from the same point after a
restart.

Java 21 + SQLite, no framework. About 1.25k lines of code and 41 tests.

## Example use case

The engine is tested on a marketing-style rule:

- **Match:** the user opened the app, viewed a product within X seconds, and did *not*
  check out within Y seconds.
- **Then:** send a push. If it isn't confirmed delivered within Z seconds, send an SMS
  instead.

The interesting part is that this rule can sit half-matched for a long time and has to
survive a crash or redeploy in the middle.

## Demo

```sh
./demo.sh
```

Scenario 1 starts a journey, sends a `PRODUCT_VIEWED` event, and then `kill -9`s the
process while it's waiting on the checkout window:

```
[11:21:44] user-7 launched the app, waiting 8s for a product view
[11:21:47] event: PRODUCT_VIEWED
[11:21:47] viewed a product. did they check out within 10s?

--- kill -9 62145, waiting on the checkout window ---
the process is gone. all that's left is the log:
0|WORKFLOW_STARTED
1|TIMER_STARTED
2|SIGNAL_RECEIVED
3|TIMER_CANCELLED
4|TIMER_STARTED

[11:21:52] restarted. nothing in memory, reading the log
[11:21:52] found user-7 mid-journey with 5 events
[11:21:57] no checkout in the window, the rule matched
[11:21:57] SENDING PUSH to user-7
[11:21:57] waiting 6s for a delivery confirmation
[11:22:03] push never confirmed delivered, falling back to SMS
[11:22:03] SENDING SMS FALLBACK to user-7
```

The new process has nothing in memory, only 5 rows in SQLite. From those it works out
that the product view already happened and that the checkout timer is still running. It
waits out the rest of that timer and then continues into the push/SMS part.

Scenario 2 sends `CHECKOUT` in time, so nothing is sent. Scenario 3 confirms delivery in
time, so the SMS fallback never fires.

## The workflow code

```java
Set<String> seen = new HashSet<>();
AtomicReference<String> delivery = new AtomicReference<>();
ctx.onSignal("PRODUCT_VIEWED",  e -> seen.add("PRODUCT_VIEWED"));
ctx.onSignal("CHECKOUT",        e -> seen.add("CHECKOUT"));
ctx.onSignal("DELIVERY_UPDATE", delivery::set);

// viewed a product within X
if (!ctx.awaitUntil(viewWindow, () -> seen.contains("PRODUCT_VIEWED"))) {
    return;
}
// and did NOT check out within Y
if (ctx.awaitUntil(checkoutWindow, () -> seen.contains("CHECKOUT"))) {
    return;
}

ctx.executeActivity("sendPush", userId);
if (ctx.awaitUntil(deliveryWindow, () -> "VENDOR_DELIVERED".equals(delivery.get()))) {
    return;
}
ctx.executeActivity("sendSms", userId);
```

`awaitUntil` returns `true` if the condition became true first and `false` if the timer
ran out first. "Did X happen within N seconds" and "did X *not* happen" are just the two
branches of the same call. `seen` and `delivery` are normal local variables.

## How it works

A half-finished workflow has local variables and is somewhere inside an if/else. Java
can't serialize a stack frame, so that state can't just be saved to disk.

Instead, the engine **replays** the workflow:

1. Every decision the workflow makes (start a timer, run an activity, etc.) is appended
   to an event log in SQLite.
2. To continue a workflow, the engine calls the workflow function again from the top.
3. Each call that already has a result in the log gets that recorded result back instead
   of running again. Signals are replayed in their original order, so `seen` ends up the
   same as before the crash.
4. When the workflow reaches a call that isn't in the log yet, it unwinds. The engine runs
   that step for real, writes the result to the log, and replays again.

So the workflow function runs many times, but each activity (the actual side effect, like
sending a push) only runs once.

Recovery after a crash uses the same code path as normal execution. `recover()` just
loops over instances still marked RUNNING and calls the same `drive()` method.

### Design details

**Determinism check.** Recorded results are matched to calls by their position in the log.
If the workflow code takes a different path than last time (usually because the code was
changed while an instance was running), result #3 would no longer mean what the code
expects. The engine compares each command against the log and throws
`NonDeterministicException` if they don't match.

**Deadlines are stored, not recomputed.** `awaitUntil` reads the clock once, when the timer
is created, and saves the deadline in the event. Calling `Instant.now()` on every replay
would keep pushing the deadline forward.

**Timers and signals.** `awaitUntil` always starts the timer before checking the
condition, even if the signal already arrived. This costs an extra round sometimes, but
keeps a single path through the code, so there's no leftover timer pointing at a finished
workflow. Cancelling a timer is logged as `TIMER_CANCELLED` and checked on replay like any
other event. Signals that arrive before the wait starts still count, because handlers see
the whole history in order.

**Inputs.** The time windows are passed in as workflow input, so a workflow restarted
a week later uses the values it was started with, not whatever the current defaults are.

**Storage.** The `events` table is the source of truth. `workflow_instances` and `timers`
are indexes over it (to find running workflows and due timers without scanning every
history). They're written in the same transaction as the events and could be rebuilt from
the log.

Appends are optimistic: read the history, decide the next step, and append at the
expected next sequence number. If another process already wrote there, a
`ConflictException` is thrown and nothing is written. Two processes racing on the same
workflow can do duplicate work but can't corrupt the history. The demo depends on this,
since the event emitter and the timer poller are separate processes.

SQLite runs with WAL, `synchronous=FULL` and IMMEDIATE transactions. The reasons are
commented in `EventStore`.

## Code overview

| File | What it does |
|---|---|
| [`WorkflowContext.java`](src/main/java/dev/rahin/durable/WorkflowContext.java) | `awaitUntil`, `executeActivity`, `sleep`. Decides whether to answer from history or unwind. Best place to start reading. |
| [`WorkflowEngine.java`](src/main/java/dev/rahin/durable/WorkflowEngine.java) | Main loop: replay, collect commands, commit, run side effects, repeat. |
| [`EventStore.java`](src/main/java/dev/rahin/durable/EventStore.java) | The event log. Optimistic appends, SQLite settings, timer index. |
| [`TimerService.java`](src/main/java/dev/rahin/durable/TimerService.java) | Finds due timers and fires them. |
| [`demo/JourneyDemo.java`](src/main/java/dev/rahin/durable/demo/JourneyDemo.java) | The demo. |

## Tests

```sh
mvn test
```

Some of the more important ones:

- `aTwoWindowJourneyRuns`, `theSameJourneyStopsWhenTheUserChecksOut`: the rule matches and doesn't match correctly
- `anUndeliveredMessageFallsBackToTheNextChannel`, `aDeliveredMessageEndsTheChain`: push/SMS fallback
- `workflowCodeRerunsButEachActivityRunsOnce`: the main replay guarantee
- `aTimerFiresAfterARestartEvenThoughNothingWasInMemory`: the demo as a test
- `aSignalThatArrivedBeforeTheWaitStartedStillCounts`: signal vs. wait race
- `theRecordedFireTimeIsNotRecomputedOnReplay`: deadlines don't drift
- `changingWorkflowCodeUnderARunningWorkflowIsCaught`: non-determinism detection
- `anActivityThatRanButWasNotRecordedRunsAgain`: at-least-once behaviour (see below)
- `onlyOneAppendCanWinASequence`: 8 threads appending to the same sequence number

## Limitations

- **Replay cost grows with workflow length.** A workflow with N steps runs N+1 times, and
  each run reads the full history. Fine for this rule, not for workflows with hundreds of
  steps. Temporal avoids this by keeping the workflow paused in memory instead of
  replaying each time. This would be the first thing to improve.
- **Activities are at-least-once.** The side effect runs before its result is recorded, so
  a crash in between makes it run again (there's a test for this). Recording first would
  risk losing the activity completely, which seemed worse. A proper fix would be an
  idempotency key checked by the downstream service.
- **No retries.** A failed activity throws straight into the workflow.
- **No per-type signal buffer.** Handlers see each payload as the log replays, so "was the
  last delivery update DELIVERED" works, but "did any event have amount > 500" has to be
  tracked by the workflow itself.
- **Simple payload format.** Two strings split on the first newline, to avoid adding
  Jackson. Good enough here, not for production.
- **Polling timers.** Due timers are checked every 200 ms instead of sleeping until the
  next deadline.
- **Single process.** There's no leasing or task queue. Two engines on the same database
  won't corrupt anything, but they will both do the same work.

Out of scope on purpose: distributed workers, sharding, workflow versioning and a UI.
This project only covers the replay mechanism those features are built on.

## Running

Requires Java 21+ and Maven. Dependencies are just `sqlite-jdbc` and JUnit 5. The demo
uses the `sqlite3` CLI to print the raw log, if it's installed.

```sh
mvn test     # 41 tests
./demo.sh    # all three scenarios
```
