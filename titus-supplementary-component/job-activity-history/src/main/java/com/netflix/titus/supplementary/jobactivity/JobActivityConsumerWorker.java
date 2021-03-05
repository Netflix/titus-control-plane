package com.netflix.titus.supplementary.jobactivity;

import java.time.Duration;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityConsumerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobActivityConsumerWorker {
    private ScheduleReference schedulerRef;
    private final Clock clock;
    private final TitusRuntime titusRuntime;
    private JobActivityConsumerStore consumerStore;

    private static final Logger logger = LoggerFactory.getLogger(JobActivityConsumerWorker.class);


    @Inject
    public JobActivityConsumerWorker(TitusRuntime titusRuntime,
                                     JobActivityConsumerStore consumerStore) {
        this.clock = titusRuntime.getClock();
        this.titusRuntime = titusRuntime;
        this.consumerStore = consumerStore;

        Registry registry = titusRuntime.getRegistry();
    }

    @Activator
    public void enterActiveMode() {
        ScheduleDescriptor scheduleDescriptor = ScheduleDescriptor.newBuilder()
                .withName("populateNewRecords")
                .withDescription("Drain queue and populate new records")
                .withTimeout(Duration.ofMinutes(5))
                .build();

        this.schedulerRef = titusRuntime.getLocalScheduler().schedule(
                scheduleDescriptor,
                e -> consume(),
                ExecutorsExt.namedSingleThreadExecutor(JobActivityConsumerWorker.class.getSimpleName())
        );
    }

    public void consume() {
        System.out.println("Testing");
    }

    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }


}
