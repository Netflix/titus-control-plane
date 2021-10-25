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

package com.netflix.titus.runtime.connector.jobmanager;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.jobmanager.replicator.JobDataReplicatorProvider;
import com.netflix.titus.runtime.connector.jobmanager.snapshot.JobSnapshotFactory;

public class JobManagerDataReplicationModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JobDataReplicator.class).toProvider(JobDataReplicatorProvider.class);
        bind(ReadOnlyJobOperations.class).to(CachedReadOnlyJobOperations.class);
    }

    @Provides
    @Singleton
    public JobConnectorConfiguration getJobDataReplicatorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobConnectorConfiguration.class);
    }

    @Provides
    @Singleton
    public JobDataReplicatorProvider getJobDataReplicatorProvider(JobConnectorConfiguration configuration,
                                                                  JobManagementClient client,
                                                                  @Named(JobManagerConnectorModule.KEEP_ALIVE_ENABLED)
                                                                          JobManagementClient keepAliveEnabledClient,
                                                                  JobSnapshotFactory jobSnapshotFactory,
                                                                  TitusRuntime titusRuntime) {
        return new JobDataReplicatorProvider(
                configuration,
                configuration.isKeepAliveReplicatedStreamEnabled() ? keepAliveEnabledClient : client,
                jobSnapshotFactory,
                titusRuntime
        );
    }
}
