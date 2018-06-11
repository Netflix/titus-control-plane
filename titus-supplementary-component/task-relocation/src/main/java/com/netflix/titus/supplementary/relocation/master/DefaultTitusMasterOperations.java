package com.netflix.titus.supplementary.relocation.master;

import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.runtime.connector.agent.AgentSnapshot;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.common.util.DateTimeExt.toTimeUnitString;

@Singleton
public class DefaultTitusMasterOperations implements TitusMasterOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTitusMasterOperations.class);

    private final JobDataReplicator jobDataReplicator;

    private final Subscription subscription;

    @Inject
    public DefaultTitusMasterOperations(AgentDataReplicator agentDataReplicator,
                                        JobDataReplicator jobDataReplicator) {
        this.jobDataReplicator = jobDataReplicator;

        this.subscription = Observable.interval(0, 5, TimeUnit.SECONDS).subscribe(tick -> {
            AgentSnapshot agentSnapshot = agentDataReplicator.getCurrent();
            logger.info("Agent snapshot: instanceGroups={}, instances={}, staleness={}", agentSnapshot.getInstanceGroups().size(),
                    agentSnapshot.getInstances().size(), toTimeUnitString(agentDataReplicator.getStalenessMs()));

            JobSnapshot jobSnapshot = jobDataReplicator.getCurrent();
            logger.info("Job snapshot: jobs={}, tasks={}, staleness={}", jobSnapshot.getJobs().size(),
                    jobSnapshot.getTasks().size(), toTimeUnitString(jobDataReplicator.getStalenessMs()));
        });
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }
}
