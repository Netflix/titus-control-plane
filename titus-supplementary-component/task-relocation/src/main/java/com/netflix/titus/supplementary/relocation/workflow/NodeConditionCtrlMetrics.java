package com.netflix.titus.supplementary.relocation.workflow;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.supplementary.relocation.RelocationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeConditionCtrlMetrics {
    private static final Logger logger = LoggerFactory.getLogger(NodeConditionCtrlMetrics.class);

    public static String NODE_CONDITION_METRICS_PREFIX = RelocationMetrics.METRIC_ROOT + "nodeConditionCtrl.";
    private final Gauge stalenessStatusGauge;
    private final Gauge stalenessTimeGauge;
    private final Gauge numTasksTerminated;

    public NodeConditionCtrlMetrics(Registry registry) {
        this.stalenessStatusGauge = registry.gauge(NODE_CONDITION_METRICS_PREFIX + "stalenessStatus");
        this.stalenessTimeGauge = registry.gauge(NODE_CONDITION_METRICS_PREFIX + "stalenessMs");
        numTasksTerminated = registry.gauge(NODE_CONDITION_METRICS_PREFIX + "numTasksTerminated");
    }

    void setStaleness(boolean stalenessStatus, long stalenessMs) {
        stalenessStatusGauge.set(stalenessStatus ? 1 : 0);
        stalenessTimeGauge.set(stalenessMs);
    }

    void setTasksTerminated(int tasksTerminated) {
        numTasksTerminated.set(tasksTerminated);
    }
}
