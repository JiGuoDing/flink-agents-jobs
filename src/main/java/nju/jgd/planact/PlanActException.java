package nju.jgd.planact;

/** Raised when a Plan-Act plan or execution cannot proceed safely. */
public final class PlanActException extends RuntimeException {

    public PlanActException(String message) {
        super(message);
    }

    public PlanActException(String message, Throwable cause) {
        super(message, cause);
    }
}