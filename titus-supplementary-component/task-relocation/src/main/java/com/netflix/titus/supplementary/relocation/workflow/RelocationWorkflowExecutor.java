package com.netflix.titus.supplementary.relocation.workflow;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import rx.Observable;

@Singleton
public class RelocationWorkflowExecutor {

    @Inject
    public RelocationWorkflowExecutor() {
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

    private long getDataStalenessMs() {
        return Math.max(agentDataReplicator.getStalenessMs(), Math.max(jobDataReplicator.getStalenessMs(), evictionDataReplicator.getStalenessMs()));
    }

}
