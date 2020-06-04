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

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.titus.api.scheduler.service.SchedulerService;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.DefaultSystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.DefaultSystemSoftConstraint;
import com.netflix.titus.master.scheduler.constraint.KubeConstraint;
import com.netflix.titus.master.scheduler.constraint.NoOpConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.scheduler.constraint.V3ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.endpoint.grpc.DefaultSchedulerServiceGrpc;
import com.netflix.titus.master.scheduler.fitness.networkinterface.TitusNetworkInterfaceFitnessEvaluator;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import com.netflix.titus.master.scheduler.resourcecache.DefaultAgentResourceCache;
import com.netflix.titus.master.scheduler.service.DefaultSchedulerService;


public final class SchedulerModule extends AbstractModule {

    public static final String KUBE_CONSTRAINT = "kubeConstraint";

    private static final TypeLiteral<ConstraintEvaluatorTransformer<Pair<String, String>>> V3_CONSTRAINT_EVALUATOR_TRANSFORMER_TYPE =
            new TypeLiteral<ConstraintEvaluatorTransformer<Pair<String, String>>>() {
            };

    @Override
    protected void configure() {
        bind(VMOperations.class).to(VMOperationsImpl.class);
        bind(TierSlaUpdater.class).to(DefaultTierSlaUpdater.class);
        bind(PreferentialNamedConsumableResourceEvaluator.class).to(TitusNetworkInterfaceFitnessEvaluator.class);
        bind(new TypeLiteral<SchedulingService<? extends TaskRequest>>() {
        }).to(DefaultSchedulingService.class).asEagerSingleton();
        bind(AgentResourceCache.class).to(DefaultAgentResourceCache.class);

        bind(SystemSoftConstraint.class).to(DefaultSystemSoftConstraint.class);
        bind(SystemHardConstraint.class).to(DefaultSystemHardConstraint.class);

        bind(AgentQualityTracker.class).to(ContainerFailureBasedAgentQualityTracker.class);
        bind(V3_CONSTRAINT_EVALUATOR_TRANSFORMER_TYPE).to(V3ConstraintEvaluatorTransformer.class);

        bind(SchedulerServiceGrpc.SchedulerServiceImplBase.class).to(DefaultSchedulerServiceGrpc.class);

        bind(SchedulerService.class).to(DefaultSchedulerService.class);
    }

    @Provides
    @Singleton
    public SchedulerConfiguration getSchedulerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(SchedulerConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(KUBE_CONSTRAINT)
    public SystemConstraint getKubeConstraint(MesosConfiguration mesosConfiguration,
                                              Injector injector) {
        if (mesosConfiguration.isKubeApiServerIntegrationEnabled()) {
            return injector.getInstance(KubeConstraint.class);
        }
        return new NoOpConstraint();
    }
}
