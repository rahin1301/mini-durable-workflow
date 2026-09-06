package dev.rahin.durable.demo;

import dev.rahin.durable.EventStore;
import dev.rahin.durable.TimerService;
import dev.rahin.durable.Workflow;
import dev.rahin.durable.WorkflowContext;
import dev.rahin.durable.WorkflowEngine;
import dev.rahin.durable.WorkflowStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A campaign rule with both halves a real campaign tool has: work out who qualifies, then
 * run a follow-up chain on whoever got messaged.
 *
 * <pre>
 *   matching:    launched the app
 *                  AND viewed a product within X seconds
 *                  AND did NOT check out within Y seconds
 *   engagement:  send a push
 *                  and if it isn't confirmed delivered within Z seconds, fall back to SMS
 * </pre>
 *
 * Both halves are the same primitive. {@code awaitUntil} returns true when the event won
 * the race and false when the deadline did: "did B within X" takes the true branch, "did
 * not do C within Y" takes the false one, and so does "wasn't delivered in time".
 *
 * <pre>
 *   JourneyDemo start  &lt;db&gt; &lt;userId&gt; &lt;viewSecs&gt; &lt;checkoutSecs&gt; &lt;deliverySecs&gt;
 *   JourneyDemo emit   &lt;db&gt; &lt;userId&gt; &lt;eventName&gt; [payload]
 *   JourneyDemo resume &lt;db&gt;
 * </pre>
 */
public class JourneyDemo {

    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: start <db> <user> <viewSecs> <checkoutSecs> <deliverySecs>"
                    + " | emit <db> <user> <event> [payload] | resume <db>");
            System.exit(2);
        }
        String command = args[0];
        Path db = Path.of(args[1]);

        try (EventStore store = new EventStore(db)) {
            WorkflowEngine engine = new WorkflowEngine(store);
            TimerService timers = new TimerService(store, engine);

            engine.registerWorkflow("Journey", new Journey());
            engine.registerActivity("sendPush", userId -> {
                log("SENDING PUSH to " + userId);
                return "VENDOR_SENT";
            });
            engine.registerActivity("sendSms", userId -> {
                log("SENDING SMS FALLBACK to " + userId);
                return "VENDOR_SENT";
            });

            switch (command) {
                case "start" -> {
                    String userId = args[2];
                    long view = args.length > 3 ? Long.parseLong(args[3]) : 10;
                    long checkout = args.length > 4 ? Long.parseLong(args[4]) : 15;
                    long delivery = args.length > 5 ? Long.parseLong(args[5]) : 8;
                    log("match: viewed a product within " + view + "s, and NOT checked out within "
                            + checkout + "s");
                    log("then:  push, and if not delivered within " + delivery + "s, fall back to SMS");
                    // The windows go into the workflow input, so they live in the log. A
                    // process restarting tomorrow replays the rule it was started with,
                    // not whatever the defaults have become by then.
                    engine.start(userId, "Journey", userId + ":" + view + ":" + checkout + ":" + delivery);
                    pollUntilSettled(store, timers);
                }
                case "emit" -> {
                    String userId = args[2];
                    String event = args[3];
                    String payload = args.length > 4 ? args[4] : null;
                    log("event: " + event + (payload == null ? "" : " = " + payload));
                    if (!engine.signal(userId, event, payload)) {
                        log("  (journey already finished, event dropped)");
                    }
                }
                case "resume" -> {
                    log("restarted. nothing in memory, reading the log");
                    for (String id : store.idsWithStatus(WorkflowStatus.RUNNING)) {
                        log("found " + id + " mid-journey with " + store.readHistory(id).size() + " events");
                    }
                    engine.recover();
                    pollUntilSettled(store, timers);
                }
                default -> System.err.println("unknown command: " + command);
            }
        }
    }

    private static void pollUntilSettled(EventStore store, TimerService timers) throws InterruptedException {
        while (!store.idsWithStatus(WorkflowStatus.RUNNING).isEmpty()) {
            timers.fireDueTimers();
            Thread.sleep(200);
        }
        log("journey finished");
    }

    /** The rule itself. Ordinary control flow, and all of it survives a restart. */
    private static final class Journey implements Workflow {

        @Override
        public void run(WorkflowContext ctx, String input) {
            String[] rule = input.split(":");
            String userId = rule[0];
            Duration viewWindow = Duration.ofSeconds(Long.parseLong(rule[1]));
            Duration checkoutWindow = Duration.ofSeconds(Long.parseLong(rule[2]));
            Duration deliveryWindow = Duration.ofSeconds(Long.parseLong(rule[3]));

            Set<String> seen = new HashSet<>();
            AtomicReference<String> delivery = new AtomicReference<>();
            ctx.onSignal("PRODUCT_VIEWED", event -> seen.add("PRODUCT_VIEWED"));
            ctx.onSignal("CHECKOUT", event -> seen.add("CHECKOUT"));
            ctx.onSignal("DELIVERY_UPDATE", delivery::set);

            // --- matching: who qualifies ---------------------------------------------
            say(ctx, userId + " launched the app, waiting " + viewWindow.toSeconds() + "s for a product view");
            if (!ctx.awaitUntil(viewWindow, () -> seen.contains("PRODUCT_VIEWED"))) {
                say(ctx, "never viewed a product, not our audience");
                return;
            }

            say(ctx, "viewed a product. did they check out within " + checkoutWindow.toSeconds() + "s?");
            if (ctx.awaitUntil(checkoutWindow, () -> seen.contains("CHECKOUT"))) {
                say(ctx, "they checked out, leave them alone");
                return;
            }
            say(ctx, "no checkout in the window, the rule matched");

            // --- engagement: what happens to whoever we messaged ----------------------
            ctx.executeActivity("sendPush", userId);
            say(ctx, "waiting " + deliveryWindow.toSeconds() + "s for a delivery confirmation");
            if (ctx.awaitUntil(deliveryWindow, () -> "VENDOR_DELIVERED".equals(delivery.get()))) {
                say(ctx, "push was delivered, the chain ends here");
                return;
            }
            say(ctx, "push never confirmed delivered, falling back to SMS");
            ctx.executeActivity("sendSms", userId);
        }

        // Only speak on the live edge. Everything before that is the engine re-deriving
        // decisions it already made, and repeating them would just be noise.
        private void say(WorkflowContext ctx, String message) {
            if (!ctx.isReplaying()) {
                log(message);
            }
        }
    }

    private static void log(String message) {
        System.out.println("[" + LocalTime.now().format(CLOCK) + "] " + message);
    }
}
