/*
 * Copyright 2019 Netflix, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FeatureComplianceAggregator<T> implements FeatureCompliance<T> {

    private static final Logger logger = LoggerFactory.getLogger(FeatureComplianceAggregator.class);

    private final List<FeatureCompliance<T>> delegates;

    FeatureComplianceAggregator(List<FeatureCompliance<T>> delegates) {
        Preconditions.checkArgument(delegates.size() > 1, "Composite used for less than two delegates");
        this.delegates = delegates;
    }

    @Override
    public Optional<NonComplianceList<T>> checkCompliance(T value) {
        List<NonComplianceList<T>> combinedResult = new ArrayList<>();
        delegates.forEach(d -> {
            try {
                d.checkCompliance(value).ifPresent(combinedResult::add);
            } catch (Exception e) {
                logger.warn("[{}] Unexpected error during compliance checking for value: {}", FeatureComplianceAggregator.class.getSimpleName(), value);
                combinedResult.add(NonComplianceList.of(
                        d.getClass().getSimpleName(),
                        value,
                        Collections.singletonMap("unexpectedError", e.getMessage()),
                        String.format("Unexpected error during data validation: errorMessage=%s", e.getMessage())
                ));
            }
        });
        return combinedResult.isEmpty() ? Optional.empty() : NonComplianceList.merge(combinedResult);
    }
}
