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

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.runtime.TitusRuntime;

import static com.netflix.titus.common.util.StringExt.getNonEmptyOrDefault;

public class SpectatorAdmissionController implements AdmissionController {

    private static final String METRIC_NAME = "titus.admissionController.decision";

    final AdmissionController delegate;
    private final Registry registry;

    public SpectatorAdmissionController(AdmissionController delegate, TitusRuntime titusRuntime) {
        this.delegate = delegate;
        this.registry = titusRuntime.getRegistry();
    }

    @Override
    public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
        try {
            AdmissionControllerResponse result = delegate.apply(request);

            registry.counter(METRIC_NAME,
                    "callerId", getNonEmptyOrDefault(request.getCallerId(), "requiredButNotSet"),
                    "endpointName", getNonEmptyOrDefault(request.getEndpointName(), "requiredButNotSet)"),
                    "allowed", "" + result.isAllowed(),
                    "decisionPoint", getNonEmptyOrDefault(result.getDecisionPoint(), "requiredButNotSet"),
                    "equivalenceGroup", getNonEmptyOrDefault(result.getEquivalenceGroup(), "requiredButNotSet")
            ).increment();

            return result;
        } catch (Exception e) {
            registry.counter(METRIC_NAME,
                    "callerId", getNonEmptyOrDefault(request.getCallerId(), "requiredButNotSet"),
                    "endpointName", getNonEmptyOrDefault(request.getEndpointName(), "requiredButNotSet"),
                    "error", e.getClass().getSimpleName()
            ).increment();

            throw e;
        }
    }
}
