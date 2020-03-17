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

import java.util.function.Function;
import java.util.function.Predicate;

import com.netflix.titus.common.util.feature.FeatureGuard.FeatureGuardResult;

public class FeatureGuards {

    public static <T> Predicate<T> toPredicate(FeatureGuard<T>... featureGuard) {
        if (featureGuard.length == 0) {
            return value -> false;
        }
        if (featureGuard.length == 1) {
            return value -> featureGuard[0].matches(value) == FeatureGuardResult.Approved;
        }
        return value -> {
            for (FeatureGuard<T> next : featureGuard) {
                FeatureGuardResult result = next.matches(value);
                switch (result) {
                    case Approved:
                        return true;
                    case Denied:
                        return false;
                    case Undecided:
                        // Move to the next one
                }
            }
            return false;
        };
    }

    public static FeatureGuardWhiteListBuilder newWhiteList() {
        return new FeatureGuardWhiteListBuilder();
    }

    public static FeatureGuardWhiteListBuilder newWhiteListFromConfiguration(FeatureGuardWhiteListConfiguration configuration) {
        return new FeatureGuardWhiteListBuilder()
                .withTurnOnPredicate(configuration::isFeatureEnabled)
                .withWhiteListRegExpSupplier("whiteList", configuration::getWhiteList)
                .withBlackListRegExpSupplier("blackList", configuration::getBlackList);
    }

    public static <T> FeatureGuard<T> fromField(Function<T, String> accessor, FeatureGuard<String> delegate) {
        return new FeatureGuardForField<>(accessor, delegate);
    }
}
