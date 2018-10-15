package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.List;
import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

public interface DeschedulerService {

    List<DeschedulingResult> deschedule(Map<String, TaskRelocationPlan> plannedAheadTaskRelocationPlans);
}
