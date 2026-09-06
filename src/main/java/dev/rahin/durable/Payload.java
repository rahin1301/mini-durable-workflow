package dev.rahin.durable;

/**
 * Event payloads are two fields split on the first newline. The head is always something
 * short and newline-free (an activity name, or the sequence of the event being answered)
 * and the tail is arbitrary user data. Enough for this engine and it avoids pulling in a
 * JSON library just to hold two strings.
 */
final class Payload {

    private Payload() {
    }

    static String encode(String head, String tail) {
        return head + "\n" + (tail == null ? "" : tail);
    }

    static String encode(long head, String tail) {
        return encode(Long.toString(head), tail);
    }

    static String head(String payload) {
        int split = payload.indexOf('\n');
        return split < 0 ? payload : payload.substring(0, split);
    }

    static String tail(String payload) {
        int split = payload.indexOf('\n');
        return split < 0 ? "" : payload.substring(split + 1);
    }
}
