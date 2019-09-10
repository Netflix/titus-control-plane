package com.netflix.titus.supplementary.jobactivity.store;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import reactor.core.publisher.Mono;

/*
The consumer store consumes from the publisher queue
 */
public interface JobActivityConsumerStore {
    // Consumes a job
    Mono<Void> consumeJob(Job<?> Job);

    // Consumes a task
    Mono<Void> consumeTask(Task Task);
}
