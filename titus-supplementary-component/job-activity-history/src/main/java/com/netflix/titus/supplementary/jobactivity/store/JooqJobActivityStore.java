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

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.runtime.jobactivity.JobActivityPublisherRecordUtils;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.supplementary.jobactivityhistory.generated.activity.Tables.ACTIVITY_QUEUE;
import static com.netflix.titus.supplementary.jobactivityhistory.generated.jobactivity.Jobactivity.JOBACTIVITY;

@Singleton
public class JooqJobActivityStore implements JobActivityStore {

    private static final Logger logger = LoggerFactory.getLogger(JobActivityStore.class);
    private final DSLContext jobActivityDslContext;
    private final DSLContext producerDSLContext;
    private final TitusRuntime titusRuntime;
    private ScheduleReference schedulerRef;
    private final Clock clock;

    @Inject
    public JooqJobActivityStore(TitusRuntime titusRuntime,
                                DSLContext jobActivityDSLContext,
                                DSLContext producerDSLContext) {
        this(titusRuntime, jobActivityDSLContext, producerDSLContext, true);
    }

    @VisibleForTesting
    public JooqJobActivityStore(TitusRuntime titusRuntime,
                                DSLContext jobActivityDSLContext,
                                DSLContext producerDSLContext,
                                boolean createIfNotExists) {
        this.jobActivityDslContext = jobActivityDSLContext;
        this.producerDSLContext = producerDSLContext;
        this.titusRuntime = titusRuntime;
        this.clock = titusRuntime.getClock();
        if (createIfNotExists) {
            // create schema
            createSchemaIfNotExists();
        }
    }

    public void createSchemaIfNotExists() {
        jobActivityDslContext.createSchemaIfNotExists(JOBACTIVITY);
        jobActivityDslContext.createTableIfNotExists(JOBACTIVITY.JOBS);
        jobActivityDslContext.createTableIfNotExists(JOBACTIVITY.TASKS);
    }

    @Override
    public void consumeRecords() {
        writeRecordToJobActivity(readRecordsFromPublisher());
        return;
    }

    public List<JobActivityPublisherRecord> readRecordsFromPublisher() {
        //return JooqUtils.executeAsyncMono(() -> {
        //long startTimeMs = System.currentTimeMillis();
        List<JobActivityPublisherRecord> records = producerDSLContext
                .selectFrom(ACTIVITY_QUEUE)
                .orderBy(ACTIVITY_QUEUE.QUEUE_INDEX)
                .fetchInto(JobActivityPublisherRecord.class);
        return records;
        //}, producerDSLContext).flatMapIterable(jobActivityPublisherRecords -> jobActivityPublisherRecords);
    }

    public void writeRecordToJobActivity(List<JobActivityPublisherRecord> records) {
        records.stream()
                .filter(r -> (r.getRecordType().equals(JobActivityPublisherRecord.RecordType.JOB)))
                .map(r -> {
                    try {
                        Job job = JobActivityPublisherRecordUtils.getJobFromRecord(r);
                        jobActivityDslContext.insertInto(JOBACTIVITY.JOBS,
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
