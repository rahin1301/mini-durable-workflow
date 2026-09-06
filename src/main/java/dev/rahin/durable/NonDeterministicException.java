package dev.rahin.durable;

/**
 * The workflow code issued a different command than the one recorded at the same position
 * in the history, which means replaying this history no longer reproduces the run that
 * actually happened. Usually it means the workflow code was edited while instances of it
 * were still in flight.
 */
public class NonDeterministicException extends RuntimeException {

    public NonDeterministicException(int commandIndex, String recorded, String issued) {
        super("command " + commandIndex + ": history has " + recorded + " but the code issued " + issued);
    }
}
