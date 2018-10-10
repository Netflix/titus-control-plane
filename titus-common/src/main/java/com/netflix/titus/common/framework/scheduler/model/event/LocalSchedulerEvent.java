package com.netflix.titus.common.framework.scheduler.model.event;

import java.util.Objects;

import com.netflix.titus.common.framework.scheduler.model.Schedule;

public abstract class LocalSchedulerEvent {
    private final Schedule schedule;

    LocalSchedulerEvent(Schedule schedule) {
        this.schedule = schedule;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LocalSchedulerEvent that = (LocalSchedulerEvent) o;
        return Objects.equals(schedule, that.schedule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schedule);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "schedule=" + schedule +
                '}';
    }
}
