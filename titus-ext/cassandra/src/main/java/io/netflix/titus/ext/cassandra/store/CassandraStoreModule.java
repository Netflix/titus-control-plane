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

package io.netflix.titus.ext.cassandra.store;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.api.appscale.store.AppScalePolicyStore;
import io.netflix.titus.api.jobmanager.store.JobStore;

public class CassandraStoreModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(AgentStore.class).to(CassandraAgentStore.class);
        bind(AppScalePolicyStore.class).to(CassAppScalePolicyStore.class);
        bind(JobStore.class).to(CassandraJobStore.class);
    }

    @Provides
    @Singleton
    CassandraStoreConfiguration getCassandraStoreConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CassandraStoreConfiguration.class);
    }
}
