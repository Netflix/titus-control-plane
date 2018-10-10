package com.netflix.titus.common.framework.scheduler.model.event;

import com.netflix.titus.common.framework.scheduler.model.Schedule;

public class ScheduleRemovedEvent extends LocalSchedulerEvent {
    public ScheduleRemovedEvent(Schedule schedule) {
        super(schedule);
    }
}
