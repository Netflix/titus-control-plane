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

package com.netflix.titus.master;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.protobuf.util.JsonFormat;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.mesos.MesosStatusOverrideFitAction;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.runtime.Fit;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.store.JobStoreFitAction;
import com.netflix.titus.common.framework.fit.FitAction;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.FitRegistry;
import com.netflix.titus.common.framework.fit.FitUtil;
import com.netflix.titus.common.jhiccup.JHiccupModule;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.common.util.guice.ContainerEventBusModule;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.rx.eventbus.internal.DefaultRxEventBus;
import com.netflix.titus.master.cluster.LeaderActivator;
import com.netflix.titus.master.mesos.MesosStatusOverrideFitAction;
import com.netflix.titus.master.scheduler.SchedulingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core runtime services.
 */
public class TitusRuntimeModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(TitusRuntimeModule.class);

    @Override
    protected void configure() {
        // Framework services
        install(new ContainerEventBusModule());
        install(new JHiccupModule());

        bind(FitActionInitializer.class).asEagerSingleton();
    }

    @Singleton
    @Provides
    public TitusRuntimeConfiguration getTitusRuntimeConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(TitusRuntimeConfiguration.class);
    }

    @Singleton
    @Provides
    public RxEventBus getRxEventBugs(Registry registry) {
        return new DefaultRxEventBus(registry.createId(MetricConstants.METRIC_ROOT + "eventbus."), registry);
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.INSTANCE,
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        DefaultTitusRuntime titusRuntime = new DefaultTitusRuntime(codeInvariants, registry);

        // Setup FIT component hierarchy
        FitFramework fitFramework = titusRuntime.getFitFramework();

        if (fitFramework.isActive()) {
            FitComponent root = fitFramework.getRootComponent();
            root.createChild(LeaderActivator.COMPONENT);
            root.createChild(V3JobOperations.COMPONENT);
            root.createChild(SchedulingService.COMPONENT);
            root.createChild(VirtualMachineMasterService.COMPONENT);

            // Add custom FIT actions
            FitRegistry fitRegistry = fitFramework.getFitRegistry();
            fitRegistry.registerActionKind(
                    MesosStatusOverrideFitAction.DESCRIPTOR,
                    (id, properties) -> injection -> new MesosStatusOverrideFitAction(id, properties, injection)
            );
            fitRegistry.registerActionKind(
                    JobStoreFitAction.DESCRIPTOR,
                    (id, properties) -> injection -> new JobStoreFitAction(id, properties, injection)
            );
        }

        return titusRuntime;
    }

    @Singleton
    private static class FitActionInitializer {

        @Inject
        public FitActionInitializer(TitusRuntimeConfiguration configuration,
                                    TitusRuntime titusRuntime,
                                    VirtualMachineMasterService mustRunAfterDependency) {
            FitFramework fitFramework = titusRuntime.getFitFramework();

            // Load FIT actions from configuration.
            int i = 0;
            Optional<Fit.AddAction> next;
            while ((next = toFitAddAction(configuration.getFitActions(), i)).isPresent()) {
                Fit.AddAction request = next.get();

                try {
                    FitComponent fitComponent = FitUtil.getFitComponentOrFail(fitFramework, request.getComponentId());
                    FitInjection fitInjection = FitUtil.getFitInjectionOrFail(request.getInjectionId(), fitComponent);

                    Function<FitInjection, FitAction> fitActionFactory = fitFramework.getFitRegistry().newFitActionFactory(
                            request.getActionKind(), request.getActionId(), request.getPropertiesMap()
                    );
                    fitInjection.addAction(fitActionFactory.apply(fitInjection));
                } catch (Exception e) {
                    logger.error("Cannot add FIT action to the framework", e);
                }

                i++;
            }
        }

        private Optional<Fit.AddAction> toFitAddAction(Map<String, String> actionAddRequests, int index) {
            String requestJson;
            try {
                requestJson = actionAddRequests.get(Integer.toString(index));
            } catch (Exception e) {
                logger.error("Cannot find FIT action at index {}; aborting the loading process", index);
                return Optional.empty();
            }

            try {
                Fit.AddAction.Builder builder = Fit.AddAction.newBuilder();
                JsonFormat.parser().merge(requestJson, builder);
                return Optional.of(builder.build());
            } catch (Exception e) {
                logger.error("Cannot parse FIT action add request; ignoring it: {}", requestJson, e);
                return Optional.empty();
            }
        }
    }
}
