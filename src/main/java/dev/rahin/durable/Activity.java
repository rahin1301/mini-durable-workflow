package dev.rahin.durable;

/** A side effect. Runs outside the replay loop, exactly where non-determinism is allowed. */
public interface Activity {

    String run(String input) throws Exception;
}
