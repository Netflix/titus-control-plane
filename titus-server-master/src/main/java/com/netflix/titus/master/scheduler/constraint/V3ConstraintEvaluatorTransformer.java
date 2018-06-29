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
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps V3 API constraint definitions into Fenzo model. The following constraint are available:
 * <ul>
 * <li>host</li>
 * <li>exclusiveHost</li>
 * <li>serverGroup</li>
 * <li>uniqueHost</li>
 * <li>zoneBalance</li>
 * </ul>
 */
@Singleton
public class V3ConstraintEvaluatorTransformer implements ConstraintEvaluatorTransformer<Pair<String, String>> {

    private static final Logger logger = LoggerFactory.getLogger(V3ConstraintEvaluatorTransformer.class);

    private static final int EXPECTED_NUM_ZONES = 3;

    private static final ExclusiveHostConstraint EXCLUSIVE_HOST_CONSTRAINT = new ExclusiveHostConstraint();

    private final MasterConfiguration config;
    private final TaskCache taskCache;

    @Inject
    public V3ConstraintEvaluatorTransformer(MasterConfiguration config, TaskCache taskCache) {
        this.config = config;
        this.taskCache = taskCache;
    }

    @Override
    public Optional<ConstraintEvaluator> hardConstraint(Pair<String, String> hardConstraint, Supplier<Set<String>> activeTasksGetter) {
        String name = hardConstraint.getLeft();
        String value = hardConstraint.getRight();
        switch (name.toLowerCase()) {
            case "exclusivehost":
                return "true".equals(value) ? Optional.of(EXCLUSIVE_HOST_CONSTRAINT) : Optional.empty();
            case "uniquehost":
                return "true".equals(value) ? Optional.of(new V3UniqueHostConstraint()) : Optional.empty();
            case "zonebalance":
                return "true".equals(value)
                        ? Optional.of(new V3ZoneBalancedHardConstraintEvaluator(taskCache, EXPECTED_NUM_ZONES, config.getHostZoneAttributeName()))
                        : Optional.empty();
            case "host":
            case "servergroup":
        }
        logger.error("Unknown or not supported job hard constraint: {}", name);
        return Optional.empty();
    }

    @Override
    public Optional<VMTaskFitnessCalculator> softConstraint(Pair<String, String> softConstraints, Supplier<Set<String>> activeTasksGetter) {
        String name = softConstraints.getLeft();
        String value = softConstraints.getRight();
        switch (name.toLowerCase()) {
            case "exclusivehost":
                return "true".equals(value) ? Optional.of(AsSoftConstraint.get(EXCLUSIVE_HOST_CONSTRAINT)) : Optional.empty();
            case "uniquehost":
                return "true".equals(value) ? Optional.of(AsSoftConstraint.get(new V3UniqueHostConstraint())) : Optional.empty();
            case "zonebalance":
                return "true".equals(value)
                        ? Optional.of(new V3ZoneBalancedFitnessCalculator(taskCache, EXPECTED_NUM_ZONES, config.getHostZoneAttributeName()))
                        : Optional.empty();
            case "host":
            case "servergroup":
        }
        logger.error("Unknown or not supported job hard constraint: {}", name);
        return Optional.empty();
    }
}
