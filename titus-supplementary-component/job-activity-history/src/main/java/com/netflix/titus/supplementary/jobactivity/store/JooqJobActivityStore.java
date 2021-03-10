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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobactivity.store.JobActivityPublisherRecord;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.time.Clock;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static org.jooq.impl.DSL.max;
import static com.netflix.titus.supplementary.jobactivityhistory.generated.activity.Tables.ACTIVITY_QUEUE;
import static com.netflix.titus.supplementary.jobactivityhistory.generated.jobactivity.Jobactivity.*;

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
        readRecordsFromPublisher();
        return;
    }

    public Flux<JobActivityPublisherRecord> readRecordsFromPublisher() {
        return JooqUtils.executeAsyncMono(() -> {
            long startTimeMs = System.currentTimeMillis();
            List<JobActivityPublisherRecord> records = producerDSLContext
                    .selectFrom(ACTIVITY_QUEUE)
                    .orderBy(ACTIVITY_QUEUE.QUEUE_INDEX)
                    .fetchInto(JobActivityPublisherRecord.class);
            return records;
        }, producerDSLContext).flatMapIterable(jobActivityPublisherRecords -> jobActivityPublisherRecords);

    }
    public void writeRecordToJobActivity() {}
    public void deleteRecordFromPublisher(){}

    @Override
    public void consumeJob(Job<?> Job) {
        return;
    }

    @Override
    public void consumeTask(Task Task) {
        return;
    }


}
