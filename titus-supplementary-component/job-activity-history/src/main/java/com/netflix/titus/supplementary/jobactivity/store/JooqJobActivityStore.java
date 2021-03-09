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
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            logger.info("Creating database");
        }
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
                e -> consumeRecord(),
                ExecutorsExt.namedSingleThreadExecutor(JooqJobActivityStore.class.getSimpleName())
        );
    }


    // Use titus local scheduler
/*    public Flux<JobActivityPublisherRecord> getRecords() {
        List<JobActivityPublisherRecord> records = DSL.using(producerDSLContext.configuration())
                .select(max(ACTIVITY_QUEUE.QUEUE_INDEX))
                .from(ACTIVITY_QUEUE)
                .fetchInto(JobActivityPublisherRecord.class);
        return
    }*/

    public void readRecordFromPublisher() {}
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

    @Override
    public void consumeRecord() {
        return;
    }


    @Deactivator
    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(schedulerRef, ScheduleReference::cancel);
    }

}
