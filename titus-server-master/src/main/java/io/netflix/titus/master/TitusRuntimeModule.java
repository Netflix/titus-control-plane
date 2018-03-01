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

package io.netflix.titus.master;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.common.framework.fit.Fit;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.jhiccup.JHiccupModule;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.common.util.guice.ContainerEventBusModule;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.rx.eventbus.internal.DefaultRxEventBus;
import io.netflix.titus.master.scheduler.SchedulingService;

/**
 * Core runtime services.
 */
public class TitusRuntimeModule extends AbstractModule {

    @Override
    protected void configure() {
        // Framework services
        install(new ContainerEventBusModule());
        install(new JHiccupModule());
    }

    @Singleton
    @Provides
    public RxEventBus getRxEventBugs(Registry registry) {
        return new DefaultRxEventBus(registry.createId(MetricConstants.METRIC_ROOT + "eventbus."), registry);
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Registry registry) {
        DefaultTitusRuntime titusRuntime = new DefaultTitusRuntime(registry);

        // Setup FIT component hierarchy
        FitComponent root = titusRuntime.getFit();
        root.addChild(Fit.newFitComponent(V3JobOperations.COMPONENT));
        root.addChild(Fit.newFitComponent(SchedulingService.COMPONENT));

        return titusRuntime;
    }
}
