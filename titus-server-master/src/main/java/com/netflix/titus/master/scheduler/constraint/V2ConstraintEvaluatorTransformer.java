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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.AsSoftConstraint;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.plugins.BalancedHostAttrConstraint;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.plugins.UniqueHostAttrConstraint;
import com.netflix.titus.api.model.v2.JobConstraints;
import com.netflix.titus.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class V2ConstraintEvaluatorTransformer implements ConstraintEvaluatorTransformer<JobConstraints> {

    private static final int EXPECTED_NUM_ZONES = 3;
    private static ExclusiveHostConstraint exclusiveHostConstraint = new ExclusiveHostConstraint();
    private static final Logger logger = LoggerFactory.getLogger(V2ConstraintEvaluatorTransformer.class);

    private final MasterConfiguration config;

    @Inject
    public V2ConstraintEvaluatorTransformer(MasterConfiguration config) {
        this.config = config;
    }

    @Override
    public Optional<ConstraintEvaluator> hardConstraint(JobConstraints constraint, Supplier<Set<String>> activeTasksGetter) {
        switch (constraint) {
            case ExclusiveHost:
                return Optional.of(exclusiveHostConstraint);
            case UniqueHost:
                return Optional.of(new UniqueHostAttrConstraint(s -> activeTasksGetter.get()));
            case ZoneBalance:
                return Optional.of(new BalancedHostAttrConstraint(s -> activeTasksGetter.get(), zoneAttributeName(), EXPECTED_NUM_ZONES));
            default:
                logger.error("Unknown job hard constraint " + constraint);
                return Optional.empty();
        }
    }

    @Override
    public Optional<VMTaskFitnessCalculator> softConstraint(JobConstraints constraint, Supplier<Set<String>> activeTasksGetter) {
        switch (constraint) {
            case ExclusiveHost:
                return Optional.of(AsSoftConstraint.get(exclusiveHostConstraint));
            case UniqueHost:
                return Optional.of(AsSoftConstraint.get(new UniqueHostAttrConstraint(s -> activeTasksGetter.get())));
            case ZoneBalance:
                return Optional.of(new BalancedHostAttrConstraint(s -> activeTasksGetter.get(), zoneAttributeName(), EXPECTED_NUM_ZONES).asSoftConstraint());
            default:
                logger.error("Unknown job soft constraint " + constraint);
                return Optional.empty();
        }
    }

    private String zoneAttributeName() {
        return config.getHostZoneAttributeName();
    }
}
