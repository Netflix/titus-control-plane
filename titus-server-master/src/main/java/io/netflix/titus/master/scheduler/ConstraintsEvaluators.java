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

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.AsSoftConstraint;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.plugins.BalancedHostAttrConstraint;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.plugins.UniqueHostAttrConstraint;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ConstraintsEvaluators {

    private static final int EXPECTED_NUM_ZONES = 3;
    private static ExclusiveHostConstraint exclusiveHostConstraint = new ExclusiveHostConstraint();
    private static final Logger logger = LoggerFactory.getLogger(ConstraintsEvaluators.class);

    private final MasterConfiguration config;

    @Inject
    public ConstraintsEvaluators(MasterConfiguration config) {
        this.config = config;
    }

    public ConstraintEvaluator hardConstraint(JobConstraints constraint, final Set<String> coTasks) {
        switch (constraint) {
            case ExclusiveHost:
                return exclusiveHostConstraint;
            case UniqueHost:
                return new UniqueHostAttrConstraint(s -> coTasks);
            case ZoneBalance:
                return new BalancedHostAttrConstraint(s -> coTasks, zoneAttributeName(), EXPECTED_NUM_ZONES);
            default:
                logger.error("Unknown job hard constraint " + constraint);
                return null;
        }
    }

    public VMTaskFitnessCalculator softConstraint(JobConstraints constraint, final Set<String> coTasks) {
        switch (constraint) {
            case ExclusiveHost:
                return AsSoftConstraint.get(exclusiveHostConstraint);
            case UniqueHost:
                return AsSoftConstraint.get(new UniqueHostAttrConstraint(s -> coTasks));
            case ZoneBalance:
                return new BalancedHostAttrConstraint(s -> coTasks, zoneAttributeName(), EXPECTED_NUM_ZONES).asSoftConstraint();
            default:
                logger.error("Unknown job soft constraint " + constraint);
                return null;
        }
    }

    private String zoneAttributeName() {
        return config.getHostZoneAttributeName();
    }
}
