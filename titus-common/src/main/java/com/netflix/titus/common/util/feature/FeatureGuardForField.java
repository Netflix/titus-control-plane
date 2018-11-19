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

class FeatureGuardForField<T> implements FeatureGuard<T> {

    private final Function<T, String> accessor;
    private final FeatureGuard<String> delegate;

    FeatureGuardForField(Function<T, String> accessor, FeatureGuard<String> delegate) {
        this.accessor = accessor;
        this.delegate = delegate;
    }

    @Override
    public FeatureGuardResult matches(T value) {
        if (value == null) {
            return FeatureGuardResult.Undecided;
        }
        return delegate.matches(accessor.apply(value));
    }
}
