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

package io.netflix.titus.master.job;

import javax.annotation.PreDestroy;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.fenzo.triggers.exceptions.SchedulerException;
import io.netflix.titus.master.job.worker.WorkerResubmitRateLimiter;
import io.netflix.titus.master.job.worker.WorkerStateMonitor;
import io.netflix.titus.master.job.worker.internal.DefaultWorkerResubmitRateLimiter;
import io.netflix.titus.master.job.worker.internal.DefaultWorkerStateMonitor;
import io.netflix.titus.master.store.WorkerStateObserver;
import io.netflix.titus.master.store.WorkerStateObserverImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JobModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(JobModule.class);

    @Override
    protected void configure() {
        bind(TriggerOperator.class).toProvider(TriggerOperatorProvider.class);
        bind(WorkerResubmitRateLimiter.class).to(DefaultWorkerResubmitRateLimiter.class);
        bind(WorkerStateMonitor.class).to(DefaultWorkerStateMonitor.class);
        bind(V2JobOperations.class).to(V2JobOperationsImpl.class);
        bind(WorkerStateObserver.class).to(WorkerStateObserverImpl.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public JobManagerConfiguration getJobManagerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobManagerConfiguration.class);
    }

    @Singleton
    private static class TriggerOperatorProvider implements Provider<TriggerOperator> {

        private final TriggerOperator triggerOperator;

        public TriggerOperatorProvider() {
            this.triggerOperator = new TriggerOperator(1);
            try {
                triggerOperator.initialize();
            } catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
        }

        @PreDestroy
        public void shutdown() {
            try {
                triggerOperator.destroy();
            } catch (SchedulerException e) {
                logger.warn("TriggerOperator shutdown failure", e);
            }
        }

        @Override
        public TriggerOperator get() {
            return triggerOperator;
        }
    }
}
