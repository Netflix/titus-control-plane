/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netflix.titus.testkit.perf.load.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

@Singleton
public class ActiveJobsMonitor {

    private Logger logger = LoggerFactory.getLogger(ActiveJobsMonitor.class);

    private static final long MAX_QUERY_DELAY_MS = 30_000;
    private static final long UPDATE_INTERVAL_MS = 5_000;

    private final ExecutionContext context;
    private final Scheduler scheduler;

    private volatile boolean doRun = true;
    private volatile Subscription activeSubscription;

    private final PublishSubject<ActiveJobs> activeJobsSubject = PublishSubject.create();

    @Inject
    public ActiveJobsMonitor(ExecutionContext context) {
        this(context, Schedulers.computation());
    }

    @VisibleForTesting
    ActiveJobsMonitor(ExecutionContext context, Scheduler scheduler) {
        this.context = context;
        this.scheduler = scheduler;
        scheduleJobQuery(0);
    }

    public Observable<ActiveJobs> activeJobs() {
        return activeJobsSubject;
    }

    public void shutdown() {
        if (doRun) {
            doRun = false;
            if (activeSubscription != null) {
                activeSubscription.unsubscribe();
            }
            activeJobsSubject.onCompleted();
        }
    }

    private void scheduleJobQuery(long delayMs) {
        if (!doRun) {
            return;
        }
        this.activeSubscription = Observable.timer(delayMs, TimeUnit.MILLISECONDS, scheduler)
                .flatMap(tick -> {
                    logger.info("Fetching latest jobs data");
                    return context.getClient().findAllJobs();
                })
                .toList()
                .timeout(MAX_QUERY_DELAY_MS, TimeUnit.MILLISECONDS, scheduler)
                .subscribe(
                        data -> activeJobsSubject.onNext(new ActiveJobs(System.currentTimeMillis(), toMap(data))),
                        e -> {
                            logger.warn("Jobs query error", e);
                            scheduleJobQuery(UPDATE_INTERVAL_MS);
                        },
                        () -> scheduleJobQuery(UPDATE_INTERVAL_MS)
                );
    }

    private Map<String, TitusJobInfo> toMap(List<TitusJobInfo> data) {
        Map<String, TitusJobInfo> result = new HashMap<>();
        data.forEach(j -> result.put(j.getId(), j));
        return result;
    }

    public static class ActiveJobs {
        private final long timestamp;
        private final Map<String, TitusJobInfo> jobs;

        public ActiveJobs(long timestamp, Map<String, TitusJobInfo> jobs) {
            this.timestamp = timestamp;
            this.jobs = jobs;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Map<String, TitusJobInfo> getJobs() {
            return jobs;
        }
    }
}
