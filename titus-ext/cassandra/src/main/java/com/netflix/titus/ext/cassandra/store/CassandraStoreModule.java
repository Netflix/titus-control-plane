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

package com.netflix.titus.ext.cassandra.store;

import javax.inject.Named;
import javax.inject.Singleton;

import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.appscale.store.AppScalePolicyStore;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.api.store.v2.ApplicationSlaStoreCache;
import com.netflix.titus.api.store.v2.ApplicationSlaStoreSanitizer;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_PERMISSIVE_SANITIZER;

public class CassandraStoreModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(AppScalePolicyStore.class).to(CassAppScalePolicyStore.class);
        bind(JobStore.class).to(CassandraJobStore.class);
        bind(LoadBalancerStore.class).to(CassandraLoadBalancerStore.class);
    }

    @Provides
    @Singleton
    public CassandraStoreConfiguration getCassandraStoreConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CassandraStoreConfiguration.class);
    }

    @Singleton
    @Provides
    public ApplicationSlaStore getApplicationSlaStore(CassandraStoreConfiguration configuration,
                                                      Session session,
                                                      @Named(JOB_PERMISSIVE_SANITIZER) EntitySanitizer coreModelSanitizers) {
        return new ApplicationSlaStoreCache(
                new ApplicationSlaStoreSanitizer(new CassandraApplicationSlaStore(configuration, session), coreModelSanitizers)
        );
    }
}
