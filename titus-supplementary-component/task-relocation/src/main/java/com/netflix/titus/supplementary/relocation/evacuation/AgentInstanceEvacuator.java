package com.netflix.titus.supplementary.relocation.evacuation;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionDataReplicator;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

@Singleton
public class AgentInstanceEvacuator {

    private static final Logger logger = LoggerFactory.getLogger(AgentInstanceEvacuator.class);

    private final static long STALENESS_THRESHOLD_MS = 5_000;

    private final Subscription subscription;

    private final AtomicReference<EvacuationExecutor> executorRef = new AtomicReference<>();

    private final AgentDataReplicator agentDataReplicator;
    private final JobDataReplicator jobDataReplicator;
    private final EvictionDataReplicator evictionDataReplicator;
    private final EvictionServiceClient evictionServiceClient;
    private final EvacuationMetrics metrics;

    @Inject
    public AgentInstanceEvacuator(AgentDataReplicator agentDataReplicator,
                                  JobDataReplicator jobDataReplicator,
                                  EvictionDataReplicator evictionDataReplicator,
                                  EvictionServiceClient evictionServiceClient,
                                  TitusRuntime titusRuntime) {
        this.agentDataReplicator = agentDataReplicator;
        this.jobDataReplicator = jobDataReplicator;
        this.evictionDataReplicator = evictionDataReplicator;
        this.evictionServiceClient = evictionServiceClient;
        this.metrics = new EvacuationMetrics(titusRuntime);

        this.subscription = Observable.interval(0, 5, TimeUnit.SECONDS).subscribe(tick -> {
            long dataStaleness = getDataStalenessMs();
            if (dataStaleness > STALENESS_THRESHOLD_MS) {
                logger.info("Stale data");
                metrics.setStaleness(true, dataStaleness);
            } else {
                metrics.setStaleness(false, dataStaleness);

                EvacuationExecutor executor = executorRef.get();
                if (executor == null) {
                    executorRef.set(executor = findNewAgentToEvacuate());
                } else if (executor.shouldAbortEvacuation()) {
                    executorRef.set(executor = null);
                }
                if (executor != null) {
                    metrics.setActiveEvacuation(true);
                    if (executorRef.get().evacuate()) {
                        executorRef.set(null);
                        metrics.setActiveEvacuation(false);
                    }
                } else {
                    metrics.setActiveEvacuation(false);
                    logger.info("Nothing to evacuate");
                }
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    private long getDataStalenessMs() {
        return Math.max(agentDataReplicator.getStalenessMs(), Math.max(jobDataReplicator.getStalenessMs(), evictionDataReplicator.getStalenessMs()));
    }

    private EvacuationExecutor findNewAgentToEvacuate() {
        AgentInstance bestMatch = null;
        for (AgentInstance instance : agentDataReplicator.getCurrent().getInstances()) {
            if (isCandidate(instance)) {
                if (bestMatch == null) {
                    bestMatch = instance;
                } else if (bestMatch.getOverrideStatus().getTimestamp() > instance.getOverrideStatus().getTimestamp()) {
                    bestMatch = instance;
                }
            }
        }
        if (bestMatch == null) {
            return null;
        }

        AgentInstanceGroup instanceGroup = agentDataReplicator.getCurrent().findInstanceGroup(bestMatch.getInstanceGroupId()).orElse(null);
        if (instanceGroup == null) {
            logger.warn("Found agent instance, but not its instance group: {}", bestMatch);
            return null;
        }

        return new EvacuationExecutor(agentDataReplicator, jobDataReplicator, evictionDataReplicator, evictionServiceClient, metrics, instanceGroup, bestMatch);
    }

    private boolean isCandidate(AgentInstance instance) {
        if (instance.getOverrideStatus().getState() != InstanceOverrideState.Quarantined) {
            return false;
        }
        return EvacuationExecutor.isAgentRunningTasks(jobDataReplicator, instance);
    }
}
