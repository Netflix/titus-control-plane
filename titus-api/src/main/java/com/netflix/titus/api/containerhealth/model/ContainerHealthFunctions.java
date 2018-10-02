package com.netflix.titus.api.containerhealth.model;

import com.google.common.base.Preconditions;

/**
 * Collection of functions to manipulate container health data.ł
 */
public final class ContainerHealthFunctions {

    public static ContainerHealthStatus merge(ContainerHealthStatus first, ContainerHealthStatus second) {
        Preconditions.checkArgument(first.getTaskId().equals(second.getTaskId()), "Cannot merge health status of two different tasks");

        ContainerHealthState effectiveState;
        switch (first.getState()) {
            case Healthy:
                effectiveState = second.getState();
                break;
            case Unhealthy:
                effectiveState = ContainerHealthState.Unhealthy;
                break;
            case Unknown:
                effectiveState = second.getState() == ContainerHealthState.Unhealthy
                        ? ContainerHealthState.Unhealthy
                        : ContainerHealthState.Unknown;
                break;
            case Terminated:
                effectiveState = second.getState();
                break;
            default:
                effectiveState = ContainerHealthState.Unknown;
        }

        return ContainerHealthStatus.newBuilder()
                .withTaskId(first.getTaskId())
                .withState(effectiveState)
                .withTimestamp(Math.max(first.getTimestamp(), second.getTimestamp()))
                .build();
    }
}
