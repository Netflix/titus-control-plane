/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.limiter.DefaultJobSubmitLimiter;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
import com.netflix.titus.master.jobmanager.store.ArchivedTasksGc;
import com.netflix.titus.master.jobmanager.store.ArchivedTasksGcConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V3JobManagerModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(V3JobManagerModule.class);

    private static final String STUCK_IN_STATE = "stuckInStateTokenBucketConfig";

    private static final TypeLiteral<DifferenceResolver<JobManagerReconcilerEvent>> JOB_DIFFERENCE_RESOLVER =
            new TypeLiteral<DifferenceResolver<JobManagerReconcilerEvent>>() {
            };

    @Override
    protected void configure() {
        bind(Key.get(JOB_DIFFERENCE_RESOLVER, Names.named(JobReconciliationFrameworkFactory.BATCH_RESOLVER))).to(BatchDifferenceResolver.class);
        bind(Key.get(JOB_DIFFERENCE_RESOLVER, Names.named(JobReconciliationFrameworkFactory.SERVICE_RESOLVER))).to(ServiceDifferenceResolver.class);

        bind(V3JobOperations.class).to(DefaultV3JobOperations.class);
        bind(ReadOnlyJobOperations.class).to(DefaultV3JobOperations.class);
        bind(JobSubmitLimiter.class).to(DefaultJobSubmitLimiter.class);

        bind(KubeNotificationProcessor.class).asEagerSingleton();

        bind(JobAndTaskMetrics.class).asEagerSingleton();

        bind(ArchivedTasksGc.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public VersionSupplier getVersionSupplier(TitusRuntime titusRuntime) {
        return VersionSuppliers.newInstance(titusRuntime.getClock());
    }

    @Provides
    @Singleton
    public JobManagerConfiguration getJobManagerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobManagerConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(JobManagerConfiguration.STUCK_IN_STATE_TOKEN_BUCKET)
    public TokenBucket getStuckInStateRateLimiter(@Named(STUCK_IN_STATE) FixedIntervalTokenBucketConfiguration config, TitusRuntime runtime) {
        return Limiters.createInstrumentedFixedIntervalTokenBucket(
                JobManagerConfiguration.STUCK_IN_STATE_TOKEN_BUCKET,
                config,
                currentTokenBucket -> logger.info("Detected {} token bucket configuration update: {}", JobManagerConfiguration.STUCK_IN_STATE_TOKEN_BUCKET, currentTokenBucket),
                runtime
        );
    }

    @Provides
    @Singleton
    @Named(STUCK_IN_STATE)
    public FixedIntervalTokenBucketConfiguration getStuckInStateTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titusMaster.jobManager.stuckInStateTokenBucket");
    }

    @Provides
    @Singleton
    public ArchivedTasksGcConfiguration getArchivedTasksGcConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ArchivedTasksGcConfiguration.class);
    }
}
