package io.netflix.titus.api.jobmanager.model.job.event;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;

public class JobUpdateEvent extends JobManagerEvent<Job> {
    private JobUpdateEvent(Job current, Optional<Job> previous) {
        super(current, previous);
    }

    @Override
    public String toString() {
        return "JobUpdateEvent{" +
                "current=" + getCurrent() +
                ", previous=" + getPrevious() +
                "}";
    }

    public static JobUpdateEvent newJob(Job current) {
        return new JobUpdateEvent(current, Optional.empty());
    }

    public static JobUpdateEvent jobChange(Job current, Job previous) {
        return new JobUpdateEvent(current, Optional.of(previous));
    }
}
