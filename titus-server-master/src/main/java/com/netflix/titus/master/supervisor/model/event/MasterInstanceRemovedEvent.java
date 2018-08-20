package com.netflix.titus.master.supervisor.model.event;

import java.util.Objects;

import com.netflix.titus.master.supervisor.model.MasterInstance;

public class MasterInstanceRemovedEvent extends SupervisorEvent {

    private final MasterInstance masterInstance;

    public MasterInstanceRemovedEvent(MasterInstance masterInstance) {
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
        MasterInstanceRemovedEvent that = (MasterInstanceRemovedEvent) o;
        return Objects.equals(masterInstance, that.masterInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterInstance);
    }

    @Override
    public String toString() {
        return "MasterInstanceRemovedEvent{" +
                "masterInstance=" + masterInstance +
                "} " + super.toString();
    }
}
