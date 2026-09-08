# Mini durable workflow engine

A small from-scratch version of the idea behind Temporal. Workflows wait on real time,
react to events, can get `kill -9`'d in the middle of all that, and still pick up right
where they left off when you restart them.

The thing I used to test it is a marketing rule, basically:

> **match** — user opened the app **and** viewed a product within X minutes **and** did
> **not** check out within Y minutes
>
> **then** — send a push, and if it's not confirmed delivered within Z minutes, fall back
> to SMS

I worked with Temporal for a few years and used it as a black box the whole time. I never
really got how a rule like this could sit half-matched for hours, survive a redeploy, and
just keep going like nothing happened. So I sat down and built the part that does that.

Java 21, SQLite, no framework. Around 1.3k lines, 41 tests.

## Running the demo

```sh
./demo.sh
```

It runs three scenarios. In the first one a journey starts, a `PRODUCT_VIEWED` event comes
in, and then I `kill -9` the process while it's still waiting on the checkout window:

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

The process that restarts has never seen this user before. All it has is 5 rows in a
SQLite file. From that it figures out a product view already happened, that the first
timer got cancelled, and that there's a checkout deadline still running — waits it out to
the second, and then moves into the follow-up chain that the first process never got to
see.

The other two scenarios in the demo just take the other branch each time: `CHECKOUT`
comes in before the deadline so nothing gets sent, or the delivery confirmation comes in
on time so SMS never fires.

## The rule itself, in code

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

Every branch is really the same thing underneath. `awaitUntil` gives back true if the
event won, false if the deadline won. So "did B happen within X" is the true branch, and
"did NOT do C within Y" is just the false branch of the same call, same for "wasn't
delivered in time". `seen` and `delivery` are nothing special, just plain local variables,
and the function can return from any of the three waits.

## Why this is actually hard

Saving the deadline itself isn't the hard part. The hard part is that a workflow that's
half done has local variables, is sitting at some point in an if/else, maybe it's inside a
try/catch. None of that can be turned into bytes and written to disk, Java doesn't let you
serialize a stack frame.

So instead of saving it, the engine just runs the function again from scratch.

Every decision the workflow makes gets written to a log first. To recover a workflow, the
engine calls the workflow function again from the top, and whenever it asks for something
that already happened before, it gets fed the answer from the log instead of doing it for
real. That includes signals — they get replayed in the same order they originally arrived,
so `seen` ends up with the same contents it had before the crash. Same code, same
recorded answers in, same branch taken, and it just continues from there.

When the workflow asks for something that isn't in the log yet, that call unwinds out of
the whole function. The engine notes down what was asked for, actually does that piece of
work outside of replay, writes the result to the log, and calls the function again from
the top. So the workflow function itself gets run over and over, but each activity — an
actual side effect like sending a push — only happens once.

That's also the reason recovery isn't some special separate code path. `recover()` is
just a loop over whatever instance was left RUNNING, calling the exact same `drive()`
method that runs during normal operation.

## Where to look in the code

| File | |
|---|---|
| [`WorkflowContext.java`](src/main/java/dev/rahin/durable/WorkflowContext.java) | Start here. `awaitUntil`, `executeActivity`, `sleep` — deciding whether to answer from history or unwind. |
| [`WorkflowEngine.java`](src/main/java/dev/rahin/durable/WorkflowEngine.java) | The decide loop: replay, collect commands, commit, run side effects, repeat. |
| [`EventStore.java`](src/main/java/dev/rahin/durable/EventStore.java) | The log itself. Optimistic appends, SQLite durability settings, the timer index. |
| [`TimerService.java`](src/main/java/dev/rahin/durable/TimerService.java) | Finds timers that are due and fires them. |
| [`demo/JourneyDemo.java`](src/main/java/dev/rahin/durable/demo/JourneyDemo.java) | The demo above. |

## A few notes on how it's built

**Determinism.** Recorded answers get matched up to calls by their position in the log.
If the code somehow takes a different path than it did the first time around, answer #3
doesn't mean what the code thinks it means anymore. So the engine compares every command
against what was recorded at that position and throws `NonDeterministicException` rather
than pretending everything's fine. Usually this means the workflow code got changed while
an instance of it was still running.

This is also why `awaitUntil` reads the clock exactly once — right when the timer gets
scheduled — and saves that deadline into the event. If it called `Instant.now()` again on
every replay, the deadline would keep sliding forward and the timer would basically never
fire.

**Timers vs signals.** `awaitUntil` always starts its timer before checking if a signal
already showed up, even when one obviously has. That wastes one extra round in the case
where the condition's already true, but it means there's only ever one path through the
function — so you can't end up with a cancelled timer sitting in the index and pointing at
a workflow that's already done.

Cancelling a timer is its own workflow decision too, so `TIMER_CANCELLED` takes up a slot
in the log like anything else and gets checked on replay same as everything. Signals that
show up before the wait even starts still count, because handlers see every signal in the
history in order — so a race between "the user does something" and "the workflow reaches
the wait" always resolves the same way.

The time windows for a journey get passed in as workflow input instead of hardcoded, so if
a process restarts a week later it replays the rule it was originally started with, not
whatever the defaults happen to be by then.

**What's actually the source of truth.** Just the `events` table. `workflow_instances`
and `timers` are basically indexes on top of it, so I don't have to scan every single
history to figure out what's running or what's due. They get written in the same
transaction as the events they come from, and could be rebuilt from the log if they ever
got lost.

Appends are optimistic — you read a history, figure out what should happen next, and try
to append at the next sequence number you'd expect to be free. If something else already
wrote there, you get a `ConflictException` and nothing gets written, because you can't
just merge in a decision that was made against a history that doesn't exist anymore. Two
processes racing on the same workflow end up doing duplicate work but can't actually
corrupt anything — which the demo relies on, since the process handling user events and
the one polling for timers are two separate processes.

`EventStore` runs SQLite with WAL, `synchronous=FULL`, and IMMEDIATE transactions. I left
comments next to each one explaining why it's set that way.

## Tests

41 tests, run with `mvn test`. Some of the ones that actually matter:

- `aTwoWindowJourneyRuns` / `theSameJourneyStopsWhenTheUserChecksOut` — matching works both ways
- `anUndeliveredMessageFallsBackToTheNextChannel` / `aDeliveredMessageEndsTheChain` — the follow-up chain
- `workflowCodeRerunsButEachActivityRunsOnce` — the replay contract, in one test
- `aTimerFiresAfterARestartEvenThoughNothingWasInMemory` — basically the demo, as an assertion
- `aSignalThatArrivedBeforeTheWaitStartedStillCounts` — the event/wait race
- `theRecordedFireTimeIsNotRecomputedOnReplay` — deadlines don't drift across replays
- `changingWorkflowCodeUnderARunningWorkflowIsCaught` — catches non-determinism
- `anActivityThatRanButWasNotRecordedRunsAgain` — proves the at-least-once thing below
- `onlyOneAppendCanWinASequence` — eight threads racing the same sequence at once

## Stuff that's not great / limitations

Being upfront about these instead of letting you find them yourself.

- Replay gets slower the longer a workflow runs. N steps means the workflow function runs
  N+1 times, and every run reads the whole history from scratch. Fine for a two-window
  rule, not fine for something with hundreds of steps. This is the tradeoff you get from
  unwinding the function instead of parking an actual coroutine like Temporal does. If I
  kept working on this, this is the first thing I'd fix.
- Activities are at-least-once, not exactly-once. The side effect happens before the
  result gets recorded, so if the process dies in between, it runs again after restart.
  There's literally a test that checks it runs twice. I thought about doing it the other
  way (record first, run second) but that can lose the activity completely if you die
  before it runs, which felt worse. The real fix would be giving each activity a dedup key
  the downstream service checks, I just didn't build that part.
- No retries for failed activities. If one throws, that's it, it goes straight into the
  workflow and the workflow deals with it. Retry policies would be next on the list.
- Signals hold a payload, but nothing buffers them per signal type. A handler just sees
  each payload as the log replays past it, so something like "was the last delivery update
  DELIVERED" works fine, but "did any event ever come in with amount > 500" needs the
  workflow itself to keep track of that.
- Payloads are literally just two strings split on the first newline. Didn't want to pull
  in Jackson for something that only needs to hold a name and a blob. It works, but I
  wouldn't ship it like this.
- Timers get checked by polling every 200ms instead of sleeping until the next actual
  deadline.
- Single process only. Optimistic appends stop two processes from corrupting a history,
  but there's no leasing or task queue, so if you ran two of them they'd both just do the
  same work twice.

## What this is (and isn't)

Just the core replay mechanism, nothing more, on purpose. No distributed execution, no
worker fleet, no sharding, no workflow versioning, no UI. Temporal is basically all of
those things built on top of this same replay idea. This project is just the idea, by
itself.

## Running it

```sh
mvn test     # 41 tests
./demo.sh    # all three scenarios above
```

Java 21. Only real dependencies are sqlite-jdbc and JUnit 5.
