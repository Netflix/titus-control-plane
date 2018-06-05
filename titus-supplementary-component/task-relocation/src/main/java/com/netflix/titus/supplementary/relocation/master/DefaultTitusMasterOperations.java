package com.netflix.titus.supplementary.relocation.master;

import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.runtime.connector.jobmanager.JobCache;
import com.netflix.titus.runtime.connector.jobmanager.JobCacheResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

import static com.netflix.titus.common.util.DateTimeExt.toTimeUnitString;

@Singleton
public class DefaultTitusMasterOperations implements TitusMasterOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTitusMasterOperations.class);

    private final JobCacheResolver jobCacheResolver;

    private final Subscription subscription;

    @Inject
    public DefaultTitusMasterOperations(JobCacheResolver jobCacheResolver) {
        this.jobCacheResolver = jobCacheResolver;

        this.subscription = Observable.interval(0, 5, TimeUnit.SECONDS).subscribe(tick -> {
            JobCache snapshot = jobCacheResolver.getCurrent();
            logger.info("Job snapshot: jobs={}, tasks={}, latency={}", snapshot.getJobs().size(),
                    snapshot.getTasks().size(), toTimeUnitString(jobCacheResolver.getStalenessMs()));
        });
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }
}
