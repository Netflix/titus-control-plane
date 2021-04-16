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

package com.netflix.titus.ext.jobactivityhistory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobactivity.store.JobActivityStoreException;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.DatabaseMetrics;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.ext.jooq.JooqContext;
import com.netflix.titus.ext.jooq.JooqUtils;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityStore;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static com.netflix.titus.ext.jooq.activity.Tables.ACTIVITY_QUEUE;
import static com.netflix.titus.ext.jobactivityhistory.jobactivity.jobactivity.Jobactivity.JOBACTIVITY;


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
            Flyway flyway = Flyway.configure()
                    .schemas("jobactivity")
                    .locations("classpath:db/migration/jobactivity")
                    .dataSource(jobActivityJooqContext.getDataSource())
                    .load();
            flyway.migrate();
        }
    }

    @Override
    public Mono<Void> processRecords() {
        return readRecordFromPublisherQueue()
                .flatMap(this::writeRecord)
                .flatMap(this::deleteRecordFromPublisher);
    }

    public Mono<JobActivityPublisherRecord> readRecordFromPublisherQueue() {
        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            List<JobActivityPublisherRecord> records = producerDSLContext
                    .selectFrom(ACTIVITY_QUEUE)
                    .orderBy(ACTIVITY_QUEUE.QUEUE_INDEX.asc())
                    .limit(1)
                    .fetchInto(JobActivityPublisherRecord.class);
            if (records.size() != 0) {
                System.out.println(records.get(0).getRecordType());
                return records.get(0);
            } else {
                System.out.println("no records");
                return null;
            }
        }, producerDSLContext).onErrorMap(e -> JobActivityStoreException.jobActivityUpdateRecordException("Read failed", e));
    }


    public Mono<JobActivityPublisherRecord> writeRecord(JobActivityPublisherRecord jobActivityPublisherRecord) {
        if (jobActivityPublisherRecord.getRecordType() == JobActivityPublisherRecord.RecordType.JOB) {
            try {
                Job job = JobActivityPublisherRecordUtils.getJobFromRecord(jobActivityPublisherRecord);
                logger.info("Read back job {}", JobActivityPublisherRecordUtils.getJobFromRecord(jobActivityPublisherRecord));
                return JooqUtils.executeAsyncMono(() -> {
                    int numInserts = jobActivityDSLContext
                            .insertInto(JOBACTIVITY.JOBS, JOBACTIVITY.JOBS.JOB_ID, JOBACTIVITY.JOBS.RECORD_TIME)
                            .values(job.getId(), Timestamp.valueOf(LocalDateTime.ofEpochSecond(job.getStatus().getTimestamp(), 0, ZoneOffset.UTC)))
                            .execute();
                    return jobActivityPublisherRecord;
                }, jobActivityDSLContext)
                        .onErrorMap(e -> JobActivityStoreException.jobActivityUpdateRecordException("e", e));
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Write failed");
                return null;
            }
        }
        return null;
    }

    public Mono<Void> deleteRecordFromPublisher(JobActivityPublisherRecord jobActivityPublisherRecord) {
        return JooqUtils.executeAsyncMono(() -> {
            try {
                producerDSLContext.deleteFrom(ACTIVITY_QUEUE).where(ACTIVITY_QUEUE.QUEUE_INDEX.eq(jobActivityPublisherRecord.getQueueIndex())).execute();
            } catch (Exception e) {
                System.out.println("didnt work");
            }
            return null;
        }, producerDSLContext).then();
    }

    @Override
    public Mono<Void> consumeJob(Job job) {
        return null;
    }

    @Override
    public void consumeTask(Task Task) {
        return;
    }


}
