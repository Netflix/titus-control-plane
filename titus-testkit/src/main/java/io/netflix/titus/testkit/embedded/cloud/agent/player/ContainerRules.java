package io.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.Map;
import java.util.Objects;

import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedTaskState;

class ContainerRules {

    private final Map<SimulatedTaskState, ContainerStateRule> taskStateRules;

    ContainerRules(Map<SimulatedTaskState, ContainerStateRule> taskStateRules) {
        this.taskStateRules = taskStateRules;
    }

    public Map<SimulatedTaskState, ContainerStateRule> getTaskStateRules() {
        return taskStateRules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContainerRules that = (ContainerRules) o;
        return Objects.equals(taskStateRules, that.taskStateRules);
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskStateRules);
    }

    @Override
    public String toString() {
        return "ContainerRules{" +
                "taskStateRules=" + taskStateRules +
                '}';
    }
}
