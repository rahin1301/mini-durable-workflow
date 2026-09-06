package dev.rahin.durable;

/** Thrown into the workflow when an activity it called ended in ACTIVITY_FAILED. */
public class ActivityFailedException extends RuntimeException {

    public ActivityFailedException(String activityName, String detail) {
        super("activity " + activityName + " failed: " + detail);
    }
}
