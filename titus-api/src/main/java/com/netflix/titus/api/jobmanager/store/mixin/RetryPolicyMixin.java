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

package com.netflix.titus.api.jobmanager.store.mixin;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.DelayedRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ExponentialBackoffRetryPolicy;
import com.netflix.titus.api.jobmanager.model.job.retry.ImmediateRetryPolicy;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ImmediateRetryPolicy.class, name = "immediateRetryPolicy"),
        @JsonSubTypes.Type(value = DelayedRetryPolicy.class, name = "delayedRetryPolicy"),
        @JsonSubTypes.Type(value = ExponentialBackoffRetryPolicy.class, name = "exponentialBackoffRetryPolicy"),
})
public abstract class RetryPolicyMixin {
}
