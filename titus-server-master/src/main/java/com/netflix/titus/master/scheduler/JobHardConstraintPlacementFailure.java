package com.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class JobHardConstraintPlacementFailure extends TaskPlacementFailure {

    private final Set<String> hardConstraints;

    public JobHardConstraintPlacementFailure(String taskId,
                                             int agentCount,
                                             Set<String> hardConstraints,
                                             Map<String, Object> rawData) {
        super(taskId, FailureKind.JobHardConstraint, agentCount, rawData);
        this.hardConstraints = hardConstraints;
    }

    public Set<String> getHardConstraints() {
        return hardConstraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        JobHardConstraintPlacementFailure that = (JobHardConstraintPlacementFailure) o;
        return Objects.equals(hardConstraints, that.hardConstraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hardConstraints);
    }

    @Override
    public String toString() {
        return "JobHardConstraintPlacementFailure{" +
                "taskId='" + getTaskId() + '\'' +
                ", failureKind=" + getFailureKind() +
                ", agentCount=" + getAgentCount() +
                ", hardConstraints=" + hardConstraints +
                "}";
    }
}
