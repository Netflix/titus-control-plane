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
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import com.netflix.titus.supplementary.jobactivity.store.JobActivityConsumerStore;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import reactor.core.publisher.Mono;


@Singleton
public class JooqJobActivityConsumerStore implements JobActivityConsumerStore {

    private static final Logger logger = LoggerFactory.getLogger(JobActivityConsumerStore.class);
    private final LogStorageInfo<Task> logStorageInfo;
    private final DSLContext jobActivityDslContext;
    private final DSLContext producerDSLContext;


    @Inject
    public JooqJobActivityConsumerStore(@Qualifier("jobActivityDSLContext") DSLContext jobActivityDSLContext,
                                        @Qualifier("producerDSLContext") DSLContext producerDSLContext,
                                        LogStorageInfo<Task> logStorageInfo) {
        this(jobActivityDSLContext, producerDSLContext, logStorageInfo, true);
    }

    @VisibleForTesting
    public JooqJobActivityConsumerStore (DSLContext jobActivityDSLContext,
                                        DSLContext producerDSLContext,
                                        LogStorageInfo<Task> logStorageInfo,
                                        boolean createIfNotExists) {
        this.jobActivityDslContext = jobActivityDSLContext;
        this.producerDSLContext = producerDSLContext;
        this.logStorageInfo = logStorageInfo;
        if (createIfNotExists) {
            // create schema
        }
        logger.info("Initializing consumer");
    }


    @Override
    public Mono<Void> consumeJob(Job<?> Job) {
        return null;
    }

    @Override
    public Mono<Void> consumeTask(Task Task) {
        return null;
    }
}
