package com.netflix.titus.api.jobmanager.model.job;

public enum ContainerHealth {
    // Unset means we haven't gotten any signal yet about healthiness
    Unset,
    // Unhealthy means the container is no longer passing its healthcheck
    Unhealthy,
    // Healthy means the container is passing its healthcheck
    Healthy;
}
