#!/usr/bin/env bash
# A campaign rule with both halves:
#
#   match:      viewed a product within 8s, AND did NOT check out within 10s
#   engagement: send a push, and if it isn't confirmed delivered within 6s, fall back to SMS
#
#   1. crash mid-journey, the rule still matches and the SMS fallback still fires
#   2. the user checks out, so nothing is sent
#   3. the push is confirmed delivered, so the fallback correctly does not fire
set -euo pipefail
cd "$(dirname "$0")"

VIEW=8
CHECKOUT=10
DELIVERY=6

mvn -q -B compile
if [ ! -f target/classpath.txt ]; then
  mvn -q -B dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
fi
CP="target/classes:$(cat target/classpath.txt)"
MAIN=dev.rahin.durable.demo.JourneyDemo

run() { java -cp "$CP" $MAIN "$@"; }
# background with exec so $! is the JVM itself. Backgrounding the shell function
# instead leaves java orphaned when we kill $!, and the demo silently proves nothing.
bg()  { exec java -cp "$CP" $MAIN "$@"; }
events() { sqlite3 "$1" "SELECT seq, type FROM events WHERE workflow_id='$2' ORDER BY seq;" 2>/dev/null \
             || echo "  (install sqlite3 to see the raw log)"; }

########################################################################
DB=target/journey-1.db
rm -f "$DB" "$DB"-wal "$DB"-shm
echo "######## 1: crash mid-journey, rule matches, push undelivered -> SMS ########"
echo
( bg start "$DB" user-7 $VIEW $CHECKOUT $DELIVERY ) &
PID=$!

sleep 3
run emit "$DB" user-7 PRODUCT_VIEWED

sleep 3
echo
echo "--- kill -9 $PID, waiting on the checkout window ---"
kill -9 $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
echo "the process is gone. all that's left is the log:"
events "$DB" user-7
echo

sleep 2
run resume "$DB"
echo
echo "--- final history ---"
events "$DB" user-7

########################################################################
DB=target/journey-2.db
rm -f "$DB" "$DB"-wal "$DB"-shm
echo
echo "######## 2: the user checks out, nothing is sent ###########################"
echo
( bg start "$DB" user-8 $VIEW $CHECKOUT $DELIVERY ) &
PID=$!
sleep 2
run emit "$DB" user-8 PRODUCT_VIEWED
sleep 2
run emit "$DB" user-8 CHECKOUT
wait $PID 2>/dev/null || true
echo
echo "--- final history (two cancelled timers, no send) ---"
events "$DB" user-8

########################################################################
DB=target/journey-3.db
rm -f "$DB" "$DB"-wal "$DB"-shm
echo
echo "######## 3: push confirmed delivered, so no SMS fallback ###################"
echo
( bg start "$DB" user-9 $VIEW $CHECKOUT $DELIVERY ) &
PID=$!
sleep 2
run emit "$DB" user-9 PRODUCT_VIEWED
sleep 11   # let the checkout window lapse so the push goes out
run emit "$DB" user-9 DELIVERY_UPDATE VENDOR_DELIVERED
wait $PID 2>/dev/null || true
echo
echo "--- final history (push only, no SMS) ---"
events "$DB" user-9
