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

package io.netflix.titus.master.loadbalancer;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import io.netflix.titus.api.loadbalancer.model.sanitizer.DefaultLoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerValidationConfiguration;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.master.loadbalancer.endpoint.grpc.DefaultLoadBalancerServiceGrpc;
import io.netflix.titus.master.loadbalancer.service.DefaultLoadBalancerService;
import io.netflix.titus.master.loadbalancer.service.LoadBalancerConfiguration;
import io.netflix.titus.runtime.store.v3.memory.InMemoryLoadBalancerStore;

public class LoadBalancerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(LoadBalancerServiceGrpc.LoadBalancerServiceImplBase.class).to(DefaultLoadBalancerServiceGrpc.class);
        bind(LoadBalancerService.class).to(DefaultLoadBalancerService.class);
        bind(LoadBalancerStore.class).to(InMemoryLoadBalancerStore.class);
        bind(LoadBalancerJobValidator.class).to(DefaultLoadBalancerJobValidator.class);
    }

    @Provides
    @Singleton
    public LoadBalancerConfiguration getLoadBalancerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(LoadBalancerConfiguration.class);
    }

    @Provides
    @Singleton
    public LoadBalancerValidationConfiguration getLoadBalancerValidationConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(LoadBalancerValidationConfiguration.class);
    }
}
