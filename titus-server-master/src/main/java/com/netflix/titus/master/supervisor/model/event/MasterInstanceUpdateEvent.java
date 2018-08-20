package com.netflix.titus.master.supervisor.model.event;

import java.util.Objects;

import com.netflix.titus.master.supervisor.model.MasterInstance;

public class MasterInstanceUpdateEvent extends SupervisorEvent {

    private final MasterInstance masterInstance;

    public MasterInstanceUpdateEvent(MasterInstance masterInstance) {
        this.masterInstance = masterInstance;
    }

    public MasterInstance getMasterInstance() {
        return masterInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MasterInstanceUpdateEvent that = (MasterInstanceUpdateEvent) o;
        return Objects.equals(masterInstance, that.masterInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterInstance);
    }

    @Override
    public String toString() {
        return "MasterInstanceUpdateEvent{" +
                "masterInstance=" + masterInstance +
                "} " + super.toString();
    }
}
