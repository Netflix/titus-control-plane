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

package io.netflix.titus.master.scheduler;

import java.util.Collections;
import java.util.Map;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.titus.grpc.protogen.SchedulerServiceGrpc;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.scheduler.service.SchedulerService;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.DefaultSystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.DefaultSystemSoftConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import io.netflix.titus.master.scheduler.constraint.V2ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.V3ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.endpoint.grpc.DefaultSchedulerServiceGrpc;
import io.netflix.titus.master.scheduler.fitness.networkinterface.TitusNetworkInterfaceFitnessEvaluator;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.DefaultAgentResourceCache;
import io.netflix.titus.master.scheduler.scaling.AutoScaleController;
import io.netflix.titus.master.scheduler.scaling.DefaultAutoScaleController;
import io.netflix.titus.master.scheduler.scaling.TitusInactiveClusterScaleDownConstraintEvaluator;
import io.netflix.titus.master.scheduler.scaling.ZoneBalancedClusterScaleDownConstraintEvaluator;
import io.netflix.titus.master.scheduler.service.DefaultSchedulerService;


public final class SchedulerModule extends AbstractModule {

    private static final TypeLiteral<ConstraintEvaluatorTransformer<JobConstraints>> V2_CONSTRAINT_EVALUATOR_TRANSFORMER_TYPE =
            new TypeLiteral<ConstraintEvaluatorTransformer<JobConstraints>>() {
            };

    private static final TypeLiteral<ConstraintEvaluatorTransformer<Pair<String, String>>> V3_CONSTRAINT_EVALUATOR_TRANSFORMER_TYPE =
            new TypeLiteral<ConstraintEvaluatorTransformer<Pair<String, String>>>() {
            };

    @Override
    protected void configure() {
        bind(VMOperations.class).to(VMOperationsImpl.class);
        bind(TierSlaUpdater.class).to(DefaultTierSlaUpdater.class);
        bind(ScaleDownOrderEvaluator.class).to(TitusInactiveClusterScaleDownConstraintEvaluator.class);
        bind(PreferentialNamedConsumableResourceEvaluator.class).to(TitusNetworkInterfaceFitnessEvaluator.class);
        bind(AutoScaleController.class).to(DefaultAutoScaleController.class);
        bind(SchedulingService.class).to(DefaultSchedulingService.class).asEagerSingleton();
        bind(AgentResourceCache.class).to(DefaultAgentResourceCache.class);

        bind(SystemSoftConstraint.class).to(DefaultSystemSoftConstraint.class);
        bind(SystemHardConstraint.class).to(DefaultSystemHardConstraint.class);

        bind(V2_CONSTRAINT_EVALUATOR_TRANSFORMER_TYPE).to(V2ConstraintEvaluatorTransformer.class);
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
    public Map<ScaleDownConstraintEvaluator, Double> getScaleDownConstraintEvaluators(
            ZoneBalancedClusterScaleDownConstraintEvaluator zoneBalancedEvaluator) {
        return Collections.singletonMap(zoneBalancedEvaluator, 1.0);
    }
}
