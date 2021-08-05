/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.util.spectator;

import java.util.Optional;

/**
 * Provides a metric object associated with the given set of tag values. API inspired by Prometheus API.
 */
public interface MetricSelector<METRIC> {

    /**
     * Provides a metric object associated with the given set of tag values. The tag value order must match the
     * tag name order. If metric object cannot be returned (for example because tagValues length does not match tagNames
     * length), returns {@link Optional#empty()}.
     */
    Optional<METRIC> withTags(String... tagValues);
}
