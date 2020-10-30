/*
 * Copyright 2020 Netflix, Inc.
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

import java.util.Map;
import java.util.function.Function;

import com.netflix.titus.common.util.StringExt;

/**
 * A matcher that evaluates all key/value pairs from a map against a provided delegate.
 */
class FeatureGuardForMap<T> implements FeatureGuard<T> {

    private final Function<T, Map<String, String>> accessor;
    private final FeatureGuard<String> delegate;

    FeatureGuardForMap(Function<T, Map<String, String>> accessor, FeatureGuard<String> delegate) {
        this.accessor = accessor;
        this.delegate = delegate;
    }

    @Override
    public FeatureGuardResult matches(T value) {
        if (value == null) {
            return FeatureGuardResult.Undecided;
        }

        Map<String, String> attributes = accessor.apply(value);
        for (String attributeKey : attributes.keySet()) {
            String attributeValue = attributes.get(attributeKey);
            String joined = attributeKey + '=' + StringExt.nonNull(attributeValue);
            FeatureGuardResult result = delegate.matches(joined);
            switch (result) {
                case Approved:
                case Denied:
                    return result;
                case Undecided:
                    break;
            }
        }

        return FeatureGuardResult.Undecided;
    }
}
