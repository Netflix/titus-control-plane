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

import java.util.Arrays;

import com.netflix.spectator.api.Registry;

public final class FeatureComplianceTypes {

    public static <T> FeatureCompliance<T> collectComplianceMetrics(Registry registry, FeatureCompliance<T> delegate) {
        return new FeatureComplianceMetricsCollector<>(registry, delegate);
    }

    public static <T> FeatureCompliance<T> logNonCompliant(FeatureCompliance<T> delegate) {
        return new FeatureComplianceLogger<>(delegate);
    }

    @SafeVarargs
    public static <T> FeatureCompliance<T> mergeComplianceValidators(FeatureCompliance<T>... delegates) {
        return new FeatureComplianceAggregator<>(Arrays.asList(delegates));
    }
}
