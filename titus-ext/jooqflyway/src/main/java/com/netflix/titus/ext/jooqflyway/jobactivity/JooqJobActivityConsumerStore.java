package com.netflix.titus.ext.jooqflyway.jobactivity;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityConsumerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


@Singleton
public class JooqJobActivityConsumerStore implements JobActivityConsumerStore {


    private static final Logger logger = LoggerFactory.getLogger(JobActivityConsumerStore.class);

    @Override
    public Mono<Void> consumeJob(Job<?> Job) {
        return null;
    }

    @Override
    public Mono<Void> consumeTask(Task Task) {
        return null;
    }
}
