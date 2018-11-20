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

package com.netflix.titus.common.util.feature;

import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FeatureGuardWithPredicates implements FeatureGuard<String> {

    private static final Logger logger = LoggerFactory.getLogger(FeatureGuardWithPredicates.class);

    private final PredicateConfig[] predicateConfigs;

    FeatureGuardWithPredicates(PredicateConfig... predicateConfigs) {
        this.predicateConfigs = predicateConfigs;
    }

    @Override
    public FeatureGuardResult matches(String value) {
        try {
            return matchesString(value);
        } catch (Exception e) {
            logger.warn("Feature status evaluation failure: value={}, error={}", value, e.getMessage());
            logger.debug("Stack trace", e);
            return FeatureGuardResult.Denied;
        }
    }

    private FeatureGuardResult matchesString(String value) {
        for (PredicateConfig predicateConfig : predicateConfigs) {
            boolean result = predicateConfig.getPredicate().test(value);
            if (result) {
                if (predicateConfig.getStepRule() == StepRule.StopOnTrue) {
                    return predicateConfig.toResult(true);
                }
            } else {
                if (predicateConfig.getStepRule() == StepRule.StopOnFalse) {
                    return predicateConfig.toResult(false);
                }
            }
            if (predicateConfig.getStepRule() == StepRule.StopOnAny) {
                return predicateConfig.toResult(result);
            }
        }
        return FeatureGuardResult.Undecided;
    }

    enum StepRule {
        StopOnFalse,
        StopOnTrue,
        StopOnAny,
    }

    static class PredicateConfig {
        private final StepRule stepRule;
        private final Predicate<String> predicate;
        private final boolean negative;

        PredicateConfig(StepRule stepRule, Predicate<String> predicate, boolean negative) {
            this.stepRule = stepRule;
            this.predicate = predicate;
            this.negative = negative;
        }

        StepRule getStepRule() {
            return stepRule;
        }

        Predicate<String> getPredicate() {
            return predicate;
        }

        FeatureGuardResult toResult(boolean predicateResult) {
            boolean result = negative != predicateResult;
            return result ? FeatureGuardResult.Approved : FeatureGuardResult.Denied;
        }
    }
}
