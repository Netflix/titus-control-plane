package com.netflix.titus.api.containerhealth.model;

public enum ContainerHealthState {
    /**
     * The application process running in the container is healthy, and ready to handle user requests.
     */
    Healthy,

    /**
     * The application process running in the container is not running or in broken state.
     * It is not ready to handle user requests. In the job disruption budget, it should be counted as not available.
     */
    Unhealthy,

    /**
     * The health status of the application running in the container is not known.
     */
    Unknown,

    /**
     * The application/container are terminated. All tasks which are finished are assigned this state.
     */
    Terminated
}
