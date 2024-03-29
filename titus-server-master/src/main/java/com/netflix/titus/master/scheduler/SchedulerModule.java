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

package com.netflix.titus.master.scheduler;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.master.scheduler.endpoint.grpc.DefaultSchedulerServiceGrpc;


public final class SchedulerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(SchedulerServiceGrpc.SchedulerServiceImplBase.class).to(DefaultSchedulerServiceGrpc.class);
    }

    @Provides
    @Singleton
    public SchedulerConfiguration getSchedulerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(SchedulerConfiguration.class);
    }
}
