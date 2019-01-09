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

import java.util.Collections;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FeatureComplianceDecorator<T> implements FeatureCompliance<T> {

    private static final Logger logger = LoggerFactory.getLogger(FeatureComplianceDecorator.class);

    private final FeatureCompliance<T> delegate;

    protected FeatureComplianceDecorator(FeatureCompliance<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<NonComplianceList<T>> checkCompliance(T value) {
        try {
            return delegate.checkCompliance(value).map(nonCompliance -> {
                processViolations(value, nonCompliance);
                return nonCompliance;
            });
        } catch (Exception e) {
            logger.warn("[{}] Unexpected error during compliance checking for value: {}", delegate.getClass().getSimpleName(), value);
            return Optional.of(NonComplianceList.of(
                    delegate.getClass().getSimpleName(),
                    value,
                    Collections.singletonMap("unexpectedError", e.getMessage()),
                    String.format("Unexpected error during data validation: errorMessage=%s", e.getMessage())
            ));
        }
    }

    protected abstract void processViolations(T value, NonComplianceList<T> nonCompliance);
}
