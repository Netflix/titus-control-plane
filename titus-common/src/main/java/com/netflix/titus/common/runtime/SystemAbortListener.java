package com.netflix.titus.common.runtime;

/**
 * A callback API for notifying listeners about a fatal event in the system, resulting in the immediate Titus shutdown.
 */
public interface SystemAbortListener {
    void onSystemAbortEvent(SystemAbortEvent event);
}
