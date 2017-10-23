package io.netflix.titus.master.appscale.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyStatus;
import io.netflix.titus.api.appscale.service.AutoScalePolicyException;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.common.util.CollectionsExt.asSet;


public class AppScaleManagerMetrics {
    private static Logger logger = LoggerFactory.getLogger(AppScaleManagerMetrics.class);
    private final Id errorMetricId;
    private Registry registry;
    private final AtomicInteger numTargets;


    private volatile Map<String, SpectatorExt.FsmMetrics<PolicyStatus>> fsmMetricsMap;

    private final Set<PolicyStatus> TRACKED_STATES = asSet(
            PolicyStatus.Pending,
            PolicyStatus.Deleting,
            PolicyStatus.Applied,
            PolicyStatus.Deleted
    );


    public AppScaleManagerMetrics(Registry registry) {
        errorMetricId = registry.createId(METRIC_APPSCALE_ERRORS);
        fsmMetricsMap = new ConcurrentHashMap<>();
        numTargets = registry.gauge(METRIC_TITUS_APPSCALE_NUM_TARGETS, new AtomicInteger(0));
        this.registry = registry;
    }

    public static final String METRIC_APPSCALE_ERRORS = "titus.appscale.errors";
    public static final String METRIC_TITUS_APPSCALE_NUM_TARGETS = "titus.appScale.numTargets";
    public static final String METRIC_TITUS_APPSCALE_POLICY = "titus.appScale.policy.";

    private Id stateIdOf(AutoScalingPolicy autoScalingPolicy) {
        return registry.createId(METRIC_TITUS_APPSCALE_POLICY, "t.jobId", autoScalingPolicy.getJobId());
    }

    private SpectatorExt.FsmMetrics<PolicyStatus> getFsmMetricsForPolicy(AutoScalingPolicy autoScalingPolicy) {
        return fsmMetricsMap.computeIfAbsent(autoScalingPolicy.getRefId(), fsmMetrics ->
                SpectatorExt.fsmMetrics(TRACKED_STATES, stateIdOf(autoScalingPolicy), policyStatus -> false, registry));
    }


    public void reportPolicyStatusTransition(AutoScalingPolicy autoScalingPolicy, PolicyStatus targetStatus) {
        SpectatorExt.FsmMetrics<PolicyStatus> fsmMetricsForPolicy = getFsmMetricsForPolicy(autoScalingPolicy);
        fsmMetricsForPolicy.transition(targetStatus);
        logger.info("AMIT Reporting transition {} -> {}", autoScalingPolicy.getRefId(), targetStatus);
    }

    public void reportNewScalableTarget() {
        numTargets.incrementAndGet();
    }

    public void reportErrorForException(AutoScalePolicyException autoScalePolicyException) {
        registry.counter(errorMetricId.withTag("errorCode", autoScalePolicyException.getErrorCode().name())).increment();
    }

}
