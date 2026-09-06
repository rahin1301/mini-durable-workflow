# Mini durable workflow engine

A from-scratch implementation of the idea behind Temporal, small enough to read in one
sitting. Workflows wait on real-world time, react to events, get `kill -9`'d halfway
through, and carry on from exactly where they were.

The thing it's built to express is a campaign rule, in the two halves a real campaign
tool has:

> **match** — user launched the app **and** viewed a product within X minutes **and** did
> **not** check out within Y minutes
>
> **then** — send a push, and if it isn't confirmed delivered within Z minutes, fall back
> to SMS

I spent four years building that kind of thing on Temporal and used it as a black box
the whole time. I never actually understood how a rule like that could sit half-matched
for hours and survive a redeploy. So I built that part.

**Java 21 · SQLite · no framework · ~1.3k lines · 41 tests**

## The demo

```sh
./demo.sh
```

Three scenarios. The first starts a journey, sends `PRODUCT_VIEWED`, then `kill -9`s the
process while it's waiting out the checkout window:

```
[10:32:36] rule: viewed a product within 10s, and did NOT check out within 15s
[10:32:36] user-7 launched the app, waiting 10s for a product view
[10:32:39] user event: PRODUCT_VIEWED
[10:32:39] viewed a product. did they check out within 15s?

--- kill -9 87384, six seconds in and waiting on the checkout window ---
the process is gone. all that's left is the log:
0|WORKFLOW_STARTED
1|TIMER_STARTED
2|SIGNAL_RECEIVED
3|TIMER_CANCELLED
4|TIMER_STARTED

[10:41:53] restarted. nothing in memory, reading the log
[10:41:53] found user-7 mid-journey with 5 events
[10:41:58] no checkout in the window, the rule matched
[10:41:58] SENDING PUSH to user-7
[10:41:58] waiting 6s for a delivery confirmation
[10:42:04] push never confirmed delivered, falling back to SMS
[10:42:04] SENDING SMS FALLBACK to user-7
```

The second process has never seen this user. It reads five rows out of a SQLite file and
works out that a product view already happened, the first window was cancelled, and
there's a checkout deadline still to wait out — which it honours to the second, then
carries on into the follow-up chain the first process never reached.

The other two scenarios take the opposite branch at each decision: a `CHECKOUT` arriving
in time so nothing is sent at all, and a delivery confirmation arriving in time so the
SMS fallback correctly doesn't fire.

## The rule, in code

```java
Set<String> seen = new HashSet<>();
AtomicReference<String> delivery = new AtomicReference<>();
ctx.onSignal("PRODUCT_VIEWED",  e -> seen.add("PRODUCT_VIEWED"));
ctx.onSignal("CHECKOUT",        e -> seen.add("CHECKOUT"));
ctx.onSignal("DELIVERY_UPDATE", delivery::set);

// match: viewed a product within X...
if (!ctx.awaitUntil(viewWindow, () -> seen.contains("PRODUCT_VIEWED"))) {
    return;
}
// ...and did NOT check out within Y
if (ctx.awaitUntil(checkoutWindow, () -> seen.contains("CHECKOUT"))) {
    return;
}

// engagement: send, then follow up unless it lands in time
ctx.executeActivity("sendPush", userId);
if (ctx.awaitUntil(deliveryWindow, () -> "VENDOR_DELIVERED".equals(delivery.get()))) {
    return;
}
ctx.executeActivity("sendSms", userId);
```

Every branch in there is the same primitive. `awaitUntil` returns true when the event won
the race and false when the deadline did, so "did B within X" is the true branch, "did not
do C within Y" is the false one, and so is "wasn't delivered in time". `seen` and
`delivery` are ordinary local variables, and the process is free to exit at any of the
three waits.

## The problem this solves

Storing the deadline is the easy part. The hard part is that a half-matched rule has
local variables, a position in its control flow, maybe it's inside a `try/catch`. You
can't serialise a Java stack frame, so none of that can be saved.

So it isn't saved. It's rebuilt by running the function again.

Every decision the workflow makes gets appended to a log. To recover a workflow the
engine re-runs the workflow code from the top, feeding it the recorded answers when it
asks for something that already happened — including replaying the signals that arrived,
in the order they arrived, so `seen` ends up holding exactly what it held before. Same
deterministic function plus the same recorded answers puts it back in the same place,
inside the same branch. Then it carries on.

When the workflow asks for something the log *can't* answer yet, the call unwinds the
whole function. The engine writes down what it wanted, does that work outside the replay
loop, records the result, and calls the function again from the top. The workflow
function runs many times; each activity runs exactly once.

Which is why recovery isn't a special code path. `recover()` is a loop over whatever was
still RUNNING calling the same `drive()` used during normal operation.

## Where to look

| File | |
|---|---|
| [`WorkflowContext.java`](src/main/java/dev/rahin/durable/WorkflowContext.java) | **Start here.** `awaitUntil`, `executeActivity` and `sleep` deciding between answering from history and unwinding. |
| [`WorkflowEngine.java`](src/main/java/dev/rahin/durable/WorkflowEngine.java) | The decide loop: replay, collect commands, commit, run side effects, repeat. |
| [`EventStore.java`](src/main/java/dev/rahin/durable/EventStore.java) | The log. Optimistic appends, SQLite durability settings, the derived timer index. |
| [`TimerService.java`](src/main/java/dev/rahin/durable/TimerService.java) | Finds due timers and fires them. |
| [`demo/JourneyDemo.java`](src/main/java/dev/rahin/durable/demo/JourneyDemo.java) | The demo above. |

## Design notes

### Determinism

Recorded answers are matched to calls by position, so if the code takes a different path
than it did originally, answer number 3 stops meaning what the code thinks it means. The
engine checks every command against the one recorded at the same position and throws
`NonDeterministicException` rather than carrying on with a history it can't trust. That's
the case where someone edited a workflow that still had live instances.

It's also why `awaitUntil` reads the clock exactly once, when the timer is first
scheduled, and writes the deadline into the event. Calling `Instant.now()` on each replay
would push the deadline out every time and it would never fire.

### Timers racing signals

`awaitUntil` always starts its timer before looking at signals, even when a signal has
already arrived. That costs one extra decision round in the case where the condition is
satisfied immediately, and buys a single path through the function, so a cancelled timer
can never be left behind in the index pointing at a workflow that has already finished.

Cancelling is itself a workflow decision, so `TIMER_CANCELLED` takes a command slot like
anything else and gets matched on replay. Signals that arrived before the wait started
still count — the handlers are fed every signal in the history, in order, so a race
between "user event" and "workflow reaches the wait" resolves the same way every time.

The journey's windows travel in the workflow input rather than being baked into the code,
so a process that restarts tomorrow replays the rule it was started with, not whatever
the defaults have become by then.

### What's actually authoritative

Only the `events` table. `workflow_instances` and `timers` are indexes so I don't have to
scan every history to answer "what was running" and "what's due" — written in the same
transaction as the events that imply them, and rebuildable from the log if lost.

Appends are optimistic. You read a history, work out what happens next, and append at the
sequence you expect to be free. If something else got there first you get a
`ConflictException` and nothing is written, because a decision made against a history that
no longer exists can't be merged in after the fact. Two processes racing the same workflow
therefore duplicate work but can't corrupt it — the demo relies on this, since the process
delivering user events and the one polling timers are separate.

`EventStore` uses WAL, `synchronous=FULL` and IMMEDIATE transactions. There are comments
on each explaining the choice.

## What's tested

38 tests, `mvn test`. The ones that matter:

- `aTwoWindowJourneyRuns` / `theSameJourneyStopsWhenTheUserChecksOut` — matching, both ways
- `anUndeliveredMessageFallsBackToTheNextChannel` / `aDeliveredMessageEndsTheChain` — the follow-up chain
- `workflowCodeRerunsButEachActivityRunsOnce` — the replay contract in one test
- `aTimerFiresAfterARestartEvenThoughNothingWasInMemory` — the demo, as an assertion
- `aSignalThatArrivedBeforeTheWaitStartedStillCounts` — the event/wait race
- `theRecordedFireTimeIsNotRecomputedOnReplay` — deadlines don't drift across replays
- `changingWorkflowCodeUnderARunningWorkflowIsCaught` — non-determinism detection
- `anActivityThatRanButWasNotRecordedRunsAgain` — proves the at-least-once behaviour below
- `onlyOneAppendCanWinASequence` — eight threads racing the same sequence

## Known limitations

Some of these are real and I'd rather name them than have you find them.

**Replay gets more expensive as a workflow gets longer.** N steps means the workflow
function runs N+1 times and each run reads the full history. Fine for a two-window rule,
not fine for one with hundreds of steps. This is the price of unwinding the function
instead of parking a coroutine the way Temporal does, and it's the first thing I'd change.

**Activities are at-least-once.** The side effect happens before the result is recorded,
so getting killed in between means it runs again on restart. There's a test asserting it
runs twice. The alternative — record first, then run — loses the activity entirely if you
die before running it, which is worse. The proper fix is handing the activity a dedup key
the downstream service can use. I didn't do that.

**No activity retries.** A failed activity is thrown into the workflow and that's where it
ends. Retry policies would be next.

**Signals carry a payload but aren't buffered per signal** — a handler sees each payload
as it replays, so "was the last delivery update DELIVERED" works, but "did any event
arrive with amount > 500" needs the workflow to accumulate that itself.

**Payloads are two strings split on the first newline.** I didn't want to add Jackson to
hold a name and a blob. It works, but it isn't a format I'd ship.

**Timers poll every 200ms** instead of sleeping until the next deadline.

**Single process.** Optimistic appends mean two processes can't corrupt a history, but
there's no leasing or task queue, so they'd both do redundant work.

## Scope

The core mechanism only, on purpose. No distributed execution, no worker fleet, no
sharding, no workflow versioning, no UI. Temporal is largely those things built on top of
the same replay idea; this is just the idea.

## Running it

```sh
mvn test     # 41 tests
./demo.sh    # all three scenarios above
```

Java 21. Dependencies are sqlite-jdbc and JUnit 5.
