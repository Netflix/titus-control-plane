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

package com.netflix.titus.runtime.endpoint.authorization;

import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
import reactor.core.publisher.Mono;

public class AllowAllAuthorizationService implements AuthorizationService {

    private static final Mono<AuthorizationStatus> STATUS = Mono.just(AuthorizationStatus.success("Authorization not enabled"));

    @Override
    public <T> Mono<AuthorizationStatus> authorize(CallMetadata callMetadata, T object) {
        return STATUS;
    }
}
