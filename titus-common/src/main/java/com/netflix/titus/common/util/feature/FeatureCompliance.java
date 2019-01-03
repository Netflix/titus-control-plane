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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link FeatureCompliance} tracks information about invocations that could not be executed, because of
 * some incompatibility with a newly introduced feature. Instances of this class should be used in parallel with
 * the {@link FeatureGuard} instances to record information for the diagnostic purposes.
 */
public interface FeatureCompliance<T> {

    /**
     * Checks feature compliance of a given value object.
     *
     * @return {@link Optional#empty()} if compliant, non-empty if violations found.
     */
    Optional<NonComplianceSet<T>> checkCompliance(T value);


    class NonCompliance<T> {

        private final String featureId;
        private final T value;
        private final Map<String, String> context;
        private final String errorMessage;

        private NonCompliance(String featureId, T value, Map<String, String> context, String errorMessage) {
            this.featureId = featureId;
            this.value = value;
            this.context = context;
            this.errorMessage = errorMessage;
        }

        public String getFeatureId() {
            return featureId;
        }

        public T getValue() {
            return value;
        }

        public Map<String, String> getContext() {
            return context;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NonCompliance<?> that = (NonCompliance<?>) o;
            return Objects.equals(featureId, that.featureId) &&
                    Objects.equals(value, that.value) &&
                    Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(featureId, value, context);
        }

        public String toErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return "NonCompliance{" +
                    "featureId='" + featureId + '\'' +
                    ", value=" + value +
                    ", context=" + context +
                    ", errorMessage='" + errorMessage + '\'' +
                    '}';
        }
    }

    class NonComplianceSet<T> {

        private final List<NonCompliance<T>> violations;

        private NonComplianceSet(List<NonCompliance<T>> violations) {
            this.violations = violations;
        }

        public List<NonCompliance<T>> getViolations() {
            return violations;
        }

        public Optional<NonCompliance<T>> findViolation(String featureId) {
            return violations.stream().filter(v -> v.getFeatureId().equals(featureId)).findFirst();
        }

        public static <T> NonComplianceSet<T> of(String featureId, T value, Map<String, String> context, String errorMessage) {
            return new NonComplianceSet<>(Collections.singletonList(new NonCompliance<>(featureId, value, context, errorMessage)));
        }

        public static <T> Optional<NonComplianceSet<T>> merge(List<NonComplianceSet<T>> items) {
            if (items.isEmpty()) {
                return Optional.empty();
            }
            if (items.size() == 1) {
                return Optional.of(items.get(0));
            }
            List<NonCompliance<T>> violations = items.stream().flatMap(tNonComplianceViolations -> tNonComplianceViolations.getViolations().stream()).collect(Collectors.toList());
            return Optional.of(new NonComplianceSet<>(violations));
        }
    }
}
