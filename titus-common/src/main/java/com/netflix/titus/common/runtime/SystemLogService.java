package com.netflix.titus.common.runtime;

/**
 * An API for processing system events (errors, exceptions, invariant violations, important configuration changes, etc)
 * with supplementary metadata information. Those events are sent to external system(s) for further processing
 * and automated analysis and reporting.
 */
public interface SystemLogService {

    /**
     * Write an event to an external system for further processing.
     *
     * @return true if the event was accepted, false otherwise. Accepting an event does not mean that it was or will be
     * successfully delivered to an external event store. False result means that the event consumer is overloaded.
     * {@link SystemLogService} implementation may choose in the latter case to accept higher priority events over
     * the lower priority ones.
     */
    boolean write(SystemLogEvent event);
}
