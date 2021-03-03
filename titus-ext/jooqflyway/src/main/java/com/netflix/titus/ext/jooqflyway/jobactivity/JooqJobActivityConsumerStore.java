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

package com.netflix.titus.ext.jooqflyway.jobactivity;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityConsumerStore;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import static com.netflix.titus.ext.jooqflyway.generated.activity.tables.ActivityQueue.ACTIVITY_QUEUE;
import static org.jooq.impl.DSL.max;

@Singleton
public class JooqJobActivityConsumerStore implements JobActivityConsumerStore {

    private static final Logger logger = LoggerFactory.getLogger(JobActivityConsumerStore.class);
    private final DSLContext jobActivityDslContext;
    private final DSLContext producerDSLContext;


    @Inject
    public JooqJobActivityConsumerStore( JooqContext jobActivityDSLContext,
                                         DSLContext producerDSLContext) {
        this(jobActivityDSLContext.getDslContext(), producerDSLContext, true);
    }

    @VisibleForTesting
    public JooqJobActivityConsumerStore (DSLContext jobActivityDSLContext,
                                        DSLContext producerDSLContext,
                                        boolean createIfNotExists) {
        this.jobActivityDslContext = jobActivityDSLContext;
        this.producerDSLContext = producerDSLContext;
        if (createIfNotExists) {
            // create schema
            logger.info("Creating database");
        }
    }

    // Use titus local scheduler 
    public void consumeRecord(){
        // figure out how to get the schema here
        Record1<Long> record = DSL.using(producerDSLContext.configuration())
                .select(max(ACTIVITY_QUEUE.QUEUE_INDEX))
                .from(ACTIVITY_QUEUE)
                .fetchOne();

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
