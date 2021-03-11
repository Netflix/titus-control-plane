/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.supplementary.jobactivity.store;

import java.util.Collections;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.DatabaseMetrics;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.flywaydb.core.Flyway;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.supplementary.jobactivityhistory.generated.activity.Tables.ACTIVITY_QUEUE;
import static com.netflix.titus.supplementary.jobactivityhistory.generated.jobactivity.Jobactivity.JOBACTIVITY;
import static org.jooq.impl.DSL.min;

@Singleton
public class JooqJobActivityStore implements JobActivityStore {

    private static final Logger logger = LoggerFactory.getLogger(JobActivityStore.class);
    private final JooqContext jobActivityJooqContext;
    private final JooqContext producerJooqContext;
    private final TitusRuntime titusRuntime;
    private ScheduleReference schedulerRef;
    private final Clock clock;

    private final DSLContext jobActivityDSLContext;
    private final DSLContext producerDSLContext;
    private final DatabaseMetrics producerDatabaseMetrics;
    private final DatabaseMetrics jobActivityDatabaseMetrics;

    @Inject
    public JooqJobActivityStore(TitusRuntime titusRuntime,
                                JooqContext jobActivityJooqContext,
                                JooqContext producerJooqContext) {
        this(titusRuntime, jobActivityJooqContext, producerJooqContext, true);
    }

    @VisibleForTesting
    public JooqJobActivityStore(TitusRuntime titusRuntime,
                                JooqContext jobActivityJooqContext,
                                JooqContext producerJooqContext,
                                boolean createIfNotExists) {
        this.jobActivityJooqContext = jobActivityJooqContext;
        this.producerJooqContext = producerJooqContext;
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();
        this.jobActivityDSLContext = jobActivityJooqContext.getDslContext();
        this.producerDSLContext = producerJooqContext.getDslContext();
        this.producerDatabaseMetrics = new DatabaseMetrics(titusRuntime.getRegistry(), "titus", "JobActivityPublisher");
        this.jobActivityDatabaseMetrics = new DatabaseMetrics(titusRuntime.getRegistry(), "titus", "JobActivityHistory");

        initializeSchema(createIfNotExists);
    }

    public void initializeSchema(boolean createIfNotExists) {
        if (createIfNotExists) {
            logger.info("Creating/migrating JooqJobStore DB schema...");
            Flyway flyway = Flyway.configure().dataSource(jobActivityJooqContext.getDataSource()).load();
            flyway.migrate();
        }
    }

    @Override
    public void consumeRecords() {
        readRecordFromPublisherQueue().subscribe(record -> System.out.println(record.getRecordType()));
    }

    public Mono<JobActivityPublisherRecord> readRecordFromPublisherQueue() {
        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            List<JobActivityPublisherRecord> records = producerDSLContext
                    .selectFrom(ACTIVITY_QUEUE)
                    .orderBy(ACTIVITY_QUEUE.QUEUE_INDEX.asc())
                    .limit(1)
                    .fetchInto(JobActivityPublisherRecord.class);
            System.out.println(records.size());
            if(records.size() != 0) {
                return records.get(0);
            } else { return null; }
        }, producerDSLContext);
    }

    /*
    public void writeRecordToJobActivity(JobActivityPublisherRecord record) {
        record.stream()
                .filter(r -> (r.getRecordType().equals(JobActivityPublisherRecord.RecordType.JOB)))
                .map(r -> {
                    try {
                        Job job = JobActivityPublisherRecordUtils.getJobFromRecord(r);
                        jobActivityDSLContext.insertInto(JOBACTIVITY.JOBS,
                                JOBACTIVITY.JOBS.JOB_ID).values(job.getId()).execute();
//, new Timestamp(job.getStatus().getTimestamp())
                        producerDSLContext.deleteFrom(ACTIVITY_QUEUE).where(ACTIVITY_QUEUE.QUEUE_INDEX.eq(r.getQueueIndex())).execute();
                    } catch (Exception e) {
                        logger.info("Something went wrong");
                    }
                    return null;
                });
        return;
    }
*/
    public void deleteRecordFromPublisher() {
    }

    @Override
    public void consumeJob(Job<?> Job) {
        return;
    }

    @Override
    public void consumeTask(Task Task) {
        return;
    }


}
