package com.netflix.titus.common.framework.scheduler.model.event;

import com.netflix.titus.common.framework.scheduler.model.Schedule;

public class ScheduleAddedEvent extends LocalSchedulerEvent {

    public ScheduleAddedEvent(Schedule schedule) {
        super(schedule);
    }
}
