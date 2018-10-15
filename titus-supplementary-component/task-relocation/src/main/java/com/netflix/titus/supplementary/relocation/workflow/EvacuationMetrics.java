package com.netflix.titus.supplementary.relocation.workflow;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.supplementary.relocation.RelocationMetrics;

class EvacuationMetrics {

    public static String EVACUATION_METRICS = RelocationMetrics.METRIC_ROOT + "evacuation.";

    private final Registry registry;

    private final Gauge stalenessStatusGauge;
    private final Gauge stalenessTimeGauge;
    private final Gauge activeEvacuation;

    EvacuationMetrics(TitusRuntime titusRuntime) {
        this.registry = titusRuntime.getRegistry();
        this.stalenessStatusGauge = registry.gauge(EVACUATION_METRICS + "stalenessStatus");
        this.stalenessTimeGauge = registry.gauge(EVACUATION_METRICS + "stalenessMs");
        this.activeEvacuation = registry.gauge(EVACUATION_METRICS + "activeEvacuation");
    }

    void setStaleness(boolean stalenessStatus, long stalenessMs) {
        stalenessStatusGauge.set(stalenessStatus ? 1 : 0);
        stalenessTimeGauge.set(stalenessMs);
    }

    void setActiveEvacuation(boolean active) {
        activeEvacuation.set(active ? 1 : 0);
    }

    void evictionSucceeded(Task task) {
        registry.counter(EVACUATION_METRICS + "evictions",
                "status", "success",
                "taskId", task.getId()
        ).increment();
    }

    void evictionFailure(Task task, Throwable error) {
        registry.counter(EVACUATION_METRICS + "evictions",
                "status", "error",
                "cause", error.getClass().getSimpleName(),
                "taskId", task.getId()
        ).increment();
    }
}
