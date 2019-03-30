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

package com.netflix.titus.api.appscale.store.mixin;


import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.appscale.model.ComparisonOperator;
import com.netflix.titus.api.appscale.model.Statistic;

public abstract class AlarmConfigurationMixIn {
    @JsonCreator
    AlarmConfigurationMixIn(
            @JsonProperty("name") String name,
            @JsonProperty("region") String region,
            @JsonProperty("actionsEnabled") Optional<Boolean> actionsEnabled,
            @JsonProperty("comparisonOperator") ComparisonOperator comparisonOperator,
            @JsonProperty("evaluationPeriods") int evaluationPeriods,
            @JsonProperty("period") int period,
            @JsonProperty("threshold") double threshold,
            @JsonProperty("metricNamespace") String metricNamespace,
            @JsonProperty("metricName") String metricName,
            @JsonProperty("statistic") Statistic statistic) {
    }

}
