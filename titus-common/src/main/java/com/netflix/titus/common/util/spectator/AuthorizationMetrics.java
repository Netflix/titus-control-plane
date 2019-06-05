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

package com.netflix.titus.common.util.spectator;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;

public class AuthorizationMetrics {
    private static final String AUTHORIZATION_METRICS_ROOT = "titus.authorization.";
    private static final String AUTHORIZATION_RESULT_TAG = "result";
    private static final String AUTHORIZATION_RESOURCE_TAG = "resource";
    private static final String AUTHORIZATION_DOMAIN_TAG = "domain";
    private static final String AUTHORIZATION_ERROR_TAG = "error";

    private final Registry registry;
    private final Id authorizationId;

    public AuthorizationMetrics(String authorizationName, Registry registry) {
        this.registry = registry;
        authorizationId = registry.createId(AUTHORIZATION_METRICS_ROOT + authorizationName);
    }

    public void incrementAuthorizationSuccess(String resourceName, String securityDomain) {
        registry.counter(authorizationId
                .withTag(AUTHORIZATION_RESULT_TAG, "success")
                .withTag(AUTHORIZATION_RESOURCE_TAG, resourceName)
                .withTag(AUTHORIZATION_DOMAIN_TAG, securityDomain)
        ).increment();
    }

    public void incrementAuthorizationError(String resourceName, String securityDomain, String errorReason) {
        registry.counter(authorizationId
                .withTag(AUTHORIZATION_RESULT_TAG, "failure")
                .withTag(AUTHORIZATION_RESOURCE_TAG, resourceName)
                .withTag(AUTHORIZATION_DOMAIN_TAG, securityDomain)
                .withTag(AUTHORIZATION_ERROR_TAG, errorReason)
        ).increment();
    }
}
