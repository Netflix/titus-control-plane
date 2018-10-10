package com.netflix.titus.common.framework.scheduler.model.event;

import com.netflix.titus.common.framework.scheduler.model.Schedule;

public class ScheduleUpdateEvent extends LocalSchedulerEvent {
    public ScheduleUpdateEvent(Schedule schedule) {
        super(schedule);
    }
}
