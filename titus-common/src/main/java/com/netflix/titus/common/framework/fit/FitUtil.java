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

package com.netflix.titus.common.framework.fit;

import java.util.Map;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.netflix.titus.common.util.ErrorGenerator;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.unit.TimeUnitExt;

/**
 * A collection of helper functions useful when building custom FIT actions.
 */
public class FitUtil {

    public static final Map<String, String> PERIOD_ERROR_PROPERTIES = ImmutableMap.of(
            "percentage", "Percentage of requests to fail (defaults to 50)",
            "errorTime", "Time window during which requests will fail (defaults to '1m')",
            "upTime", "Time window during which no errors occur (defaults to '1m')"
    );

    public static FitComponent getFitComponentOrFail(FitFramework fitFramework, String componentId) {
        return fitFramework.getRootComponent()
                .findChild(componentId)
                .orElseThrow(() -> new IllegalArgumentException("FIT component not found: " + componentId));
    }

    public static FitInjection getFitInjectionOrFail(String fitInjectionId, FitComponent fitComponent) {
        return fitComponent.findInjection(fitInjectionId)
                .orElseThrow(() -> new IllegalArgumentException("FIT injection not found: " + fitInjectionId));
    }

    public static FitAction getFitActionOrFail(String actionId, FitInjection fitInjection) {
        return fitInjection.findAction(actionId)
                .orElseThrow(() -> new IllegalArgumentException("FIT action not found: " + actionId));
    }

    public static Supplier<Boolean> periodicErrors(Map<String, String> properties) {
        double expectedRatio = Double.parseDouble(properties.getOrDefault("percentage", "100")) / 100;

        long errorTimeMs = TimeUnitExt.parse(properties.getOrDefault("errorTime", "1m"))
                .map(p -> p.getRight().toMillis(p.getLeft()))
                .orElseThrow(() -> new IllegalArgumentException("Invalid 'errorTime' parameter: " + properties.get("errorTime")));
        long upTimeMs = TimeUnitExt.parse(properties.getOrDefault("upTime", "1m"))
                .map(p -> p.getRight().toMillis(p.getLeft()))
                .orElseThrow(() -> new IllegalArgumentException("Invalid 'upTime' parameter: " + properties.get("upTime")));

        Preconditions.checkArgument(upTimeMs > 0 || errorTimeMs > 0, "Both upTime and errorTime cannot be equal to 0");

        ErrorGenerator errorGenerator = ErrorGenerator.periodicFailures(upTimeMs, errorTimeMs, expectedRatio, Clocks.system());

        return errorGenerator::shouldFail;
    }
}
