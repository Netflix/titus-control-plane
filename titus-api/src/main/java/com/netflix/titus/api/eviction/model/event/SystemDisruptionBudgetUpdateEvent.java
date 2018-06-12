package com.netflix.titus.api.eviction.model.event;

import java.util.Objects;

import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;

public class SystemDisruptionBudgetUpdateEvent extends EvictionEvent {

    private final SystemDisruptionBudget systemDisruptionBudget;

    public SystemDisruptionBudgetUpdateEvent(SystemDisruptionBudget systemDisruptionBudget) {
        this.systemDisruptionBudget = systemDisruptionBudget;
    }

    public SystemDisruptionBudget getSystemDisruptionBudget() {
        return systemDisruptionBudget;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SystemDisruptionBudgetUpdateEvent that = (SystemDisruptionBudgetUpdateEvent) o;
        return Objects.equals(systemDisruptionBudget, that.systemDisruptionBudget);
    }

    @Override
    public int hashCode() {
        return Objects.hash(systemDisruptionBudget);
    }

    @Override
    public String toString() {
        return "SystemDisruptionBudgetUpdateEvent{" +
                "systemDisruptionBudget=" + systemDisruptionBudget +
                "} " + super.toString();
    }
}
