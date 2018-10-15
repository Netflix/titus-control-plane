package com.netflix.titus.common.framework.scheduler;

import java.io.Closeable;

import com.netflix.titus.common.framework.scheduler.model.Schedule;

public interface ScheduleReference extends Closeable {

    Schedule getSchedule();

    boolean isClosed();

    @Override
    void close();
}
