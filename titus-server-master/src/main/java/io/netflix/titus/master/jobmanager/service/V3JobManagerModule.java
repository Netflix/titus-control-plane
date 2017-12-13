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

package io.netflix.titus.master.jobmanager.service;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import io.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import io.netflix.titus.master.jobmanager.service.common.DefaultV3TaskInfoFactory;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import io.netflix.titus.master.jobmanager.service.limiter.DefaultJobSubmitLimiter;
import io.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import io.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
import org.apache.mesos.Protos;

import static io.netflix.titus.master.jobmanager.service.JobReconciliationFrameworkFactory.BATCH_RESOLVER;
import static io.netflix.titus.master.jobmanager.service.JobReconciliationFrameworkFactory.SERVICE_RESOLVER;

public class V3JobManagerModule extends AbstractModule {

    private static final TypeLiteral<DifferenceResolver<JobManagerReconcilerEvent>> JOB_DIFFERENCE_RESOLVER =
            new TypeLiteral<DifferenceResolver<JobManagerReconcilerEvent>>() {
            };

    @Override
    protected void configure() {
        bind(Key.get(JOB_DIFFERENCE_RESOLVER, Names.named(BATCH_RESOLVER))).to(BatchDifferenceResolver.class);
        bind(Key.get(JOB_DIFFERENCE_RESOLVER, Names.named(SERVICE_RESOLVER))).to(ServiceDifferenceResolver.class);

        bind(V3JobOperations.class).to(DefaultV3JobOperations.class);
        bind(JobSubmitLimiter.class).to(DefaultJobSubmitLimiter.class);

        bind(new TypeLiteral<TaskInfoFactory<Protos.TaskInfo>>() {
        }).to(DefaultV3TaskInfoFactory.class);

        bind(TaskLivenessMetrics.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public JobManagerConfiguration getJobManagerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobManagerConfiguration.class);
    }
}
