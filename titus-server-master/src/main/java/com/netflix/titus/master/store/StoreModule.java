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

package com.netflix.titus.master.store;

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.titus.api.appscale.store.AppScalePolicyStore;
import com.netflix.titus.api.jobmanager.store.JobStore;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.api.store.v2.ApplicationSlaStoreCache;
import com.netflix.titus.api.store.v2.ApplicationSlaStoreSanitizer;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.master.store.memory.InMemoryApplicationSlaStore;
import com.netflix.titus.runtime.store.v3.memory.InMemoryJobStore;
import com.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;
import com.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_PERMISSIVE_SANITIZER;

public class StoreModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(JobStore.class).to(InMemoryJobStore.class);
        bind(AppScalePolicyStore.class).to(InMemoryPolicyStore.class);
        bind(LoadBalancerStore.class).to(InMemoryLoadBalancerStore.class);
    }

    @Singleton
    @Provides
    public ApplicationSlaStore getApplicationSlaStore(@Named(JOB_PERMISSIVE_SANITIZER) EntitySanitizer coreModelSanitizers) {
        return new ApplicationSlaStoreCache(
                new ApplicationSlaStoreSanitizer(new InMemoryApplicationSlaStore(), coreModelSanitizers)
        );
    }
}
