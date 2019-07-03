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

package com.netflix.titus.runtime.endpoint.admission;

import java.util.Collections;
import java.util.Map;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class ValidatorMetrics {
    private static final String VALIDATOR_METRICS_ROOT = "titus.validation.";
    private static final String VALIDATION_RESULT_TAG = "result";
    private static final String VALIDATION_RESOURCE_TAG = "resource";
    private static final String VALIDATION_ERROR_TAG = "error";

    private final Registry registry;
    private final Id validationResultId;

    public ValidatorMetrics(String validationName, Registry registry) {
        this.registry = registry;

        validationResultId = registry.createId(VALIDATOR_METRICS_ROOT + validationName);
    }

    public void incrementValidationSuccess(String resourceName) {
        registry.counter(validationResultId
                .withTag(VALIDATION_RESULT_TAG, "success")
                .withTag(VALIDATION_RESOURCE_TAG, resourceName)
        ).increment();
    }

    public void incrementValidationError(String resourceName, String errorReason) {
        incrementValidationError(resourceName, errorReason, Collections.emptyMap());
    }

    public void incrementValidationError(String resourceName, String errorReason, Map<String, String> ts) {
        registry.counter(validationResultId
                .withTags(ts)
                .withTag(VALIDATION_RESULT_TAG, "failure")
                .withTag(VALIDATION_RESOURCE_TAG, resourceName)
                .withTag(VALIDATION_ERROR_TAG, errorReason)
        ).increment();
    }

    public void incrementValidationSkipped(String resourceName, String reason) {
        registry.counter(validationResultId
                .withTag(VALIDATION_RESULT_TAG, "skipped")
                .withTag(VALIDATION_RESOURCE_TAG, resourceName)
                .withTag(VALIDATION_ERROR_TAG, reason)
        ).increment();
    }
}
