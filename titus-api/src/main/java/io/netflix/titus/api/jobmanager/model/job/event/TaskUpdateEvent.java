package io.netflix.titus.api.jobmanager.model.job.event;

import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.Task;

public class TaskUpdateEvent extends JobManagerEvent<Task> {

    private final Job<?> currentJob;
    private final Task currentTask;
    private final Optional<Task> previousTask;

    private TaskUpdateEvent(Job<?> currentJob, Task currentTask, Optional<Task> previousTask) {
        super(currentTask, previousTask);
        this.currentJob = currentJob;
        this.currentTask = currentTask;
        this.previousTask = previousTask;
    }

    public Job<?> getCurrentJob() {
        return currentJob;
    }

    public Task getCurrentTask() {
        return currentTask;
    }

    public Optional<Task> getPreviousTask() {
        return previousTask;
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

        TaskUpdateEvent that = (TaskUpdateEvent) o;

        return currentJob != null ? currentJob.equals(that.currentJob) : that.currentJob == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (currentJob != null ? currentJob.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TaskUpdateEvent{" +
                "currentJob=" + currentJob +
                ", current=" + getCurrent() +
                ", previous=" + getPrevious() +
                "}";
    }

    public static TaskUpdateEvent newTask(Job job, Task current) {
        return new TaskUpdateEvent(job, current, Optional.empty());
    }

    public static TaskUpdateEvent taskChange(Job job, Task current, Task previous) {
        return new TaskUpdateEvent(job, current, Optional.of(previous));
    }
}
