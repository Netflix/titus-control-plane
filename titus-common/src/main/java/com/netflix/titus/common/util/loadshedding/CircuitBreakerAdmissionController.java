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

package com.netflix.titus.common.util.loadshedding;

import java.util.function.Supplier;

/**
 * {@link CircuitBreakerAdmissionController} short circuits the admission process if 'enabled' returns false.
 */
public class CircuitBreakerAdmissionController implements AdmissionController {

    private static final AdmissionControllerResponse OK_FROM_CIRCUIT_BREAKER = AdmissionControllerResponse.newBuilder()
            .withAllowed(true)
            .withReasonMessage("Enforced by circuit breaker")
            .withDecisionPoint(CircuitBreakerAdmissionController.class.getSimpleName())
            .withEquivalenceGroup("all")
            .build();

    final AdmissionController delegate;
    private final Supplier<Boolean> enabled;

    public CircuitBreakerAdmissionController(AdmissionController delegate, Supplier<Boolean> enabled) {
        this.delegate = delegate;
        this.enabled = enabled;
    }

    @Override
    public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
        if (enabled.get()) {
            return delegate.apply(request);
        }
        return OK_FROM_CIRCUIT_BREAKER;
    }
}
