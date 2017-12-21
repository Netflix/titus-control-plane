package io.netflix.titus.testkit.embedded.cloud.agent;

import java.util.Objects;
import java.util.Optional;

public class AgentChangeEvent {

    public enum EventType {InstanceGroupCreated, InstanceGroupTerminated, InstanceCreated, InstanceTerminated}

    private final EventType eventType;
    private final SimulatedTitusAgentCluster instanceGroup;
    private final Optional<SimulatedTitusAgent> instance;

    private AgentChangeEvent(EventType eventType, SimulatedTitusAgentCluster instanceGroup, Optional<SimulatedTitusAgent> instance) {
        this.eventType = eventType;
        this.instanceGroup = instanceGroup;
        this.instance = instance;
    }

    public EventType getEventType() {
        return eventType;
    }

    public SimulatedTitusAgentCluster getInstanceGroup() {
        return instanceGroup;
    }

    public Optional<SimulatedTitusAgent> getInstance() {
        return instance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AgentChangeEvent that = (AgentChangeEvent) o;
        return eventType == that.eventType &&
                Objects.equals(instanceGroup, that.instanceGroup) &&
                Objects.equals(instance, that.instance);
    }

    @Override
    public int hashCode() {

        return Objects.hash(eventType, instanceGroup, instance);
    }

    @Override
    public String toString() {
        return "AgentChangeEvent{" +
                "eventType=" + eventType +
                ", instanceGroup=" + instanceGroup +
                ", instance=" + instance +
                '}';
    }

    public static AgentChangeEvent newInstanceGroup(SimulatedTitusAgentCluster instanceGroup) {
        return new AgentChangeEvent(EventType.InstanceGroupCreated, instanceGroup, Optional.empty());
    }

    public static AgentChangeEvent newInstance(SimulatedTitusAgentCluster instanceGroup, SimulatedTitusAgent instance) {
        return new AgentChangeEvent(EventType.InstanceCreated, instanceGroup, Optional.of(instance));
    }

    public static AgentChangeEvent terminatedInstanceGroup(SimulatedTitusAgentCluster instanceGroup) {
        return new AgentChangeEvent(EventType.InstanceGroupTerminated, instanceGroup, Optional.empty());
    }

    public static AgentChangeEvent terminatedInstance(SimulatedTitusAgentCluster instanceGroup, SimulatedTitusAgent instance) {
        return new AgentChangeEvent(EventType.InstanceGroupTerminated, instanceGroup, Optional.of(instance));
    }
}
