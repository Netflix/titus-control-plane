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
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.ScaleDownOrderEvaluator;
import io.netflix.titus.master.scheduler.autoscale.DefaultAutoScaleController;
import io.netflix.titus.master.scheduler.constraint.GlobalConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.TitusInactiveClusterScaleDownConstraintEvaluator;
import io.netflix.titus.master.scheduler.constraint.ZoneBalancedClusterScaleDownConstraintEvaluator;

public final class SchedulerModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(VMOperations.class).to(VMOperationsImpl.class);
        bind(TierSlaUpdater.class).to(DefaultTierSlaUpdater.class);
        bind(ScaleDownOrderEvaluator.class).to(TitusInactiveClusterScaleDownConstraintEvaluator.class);
        bind(AutoScaleController.class).to(DefaultAutoScaleController.class);
        bind(SchedulingService.class).to(DefaultSchedulingService.class).asEagerSingleton();
        bind(GlobalConstraintEvaluator.class).to(TitusGlobalConstraintEvaluator.class);
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
