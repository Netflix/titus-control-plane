package com.netflix.titus.common.runtime;

/**
 * An API for logging critical system events (errors, exceptions, invariant violations) with supplementary metadata information.
 * The events are classified by their category and priority for better searchability.
 */
public interface SystemLogService {

    enum Priority {
        Low,
        Medium,
        High
    }

    enum Category {
        RecoverableError,
        UnrecoverableError,
        ConnectivityError,
        InvariantViolation,
        Other
    }

    boolean write(Category category, String priority, String component, String message);
}
