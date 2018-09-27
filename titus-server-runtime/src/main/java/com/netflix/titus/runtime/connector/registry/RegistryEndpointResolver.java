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

package com.netflix.titus.runtime.connector.registry;

import java.net.URI;

import com.netflix.titus.common.network.client.RxRestClient;
import rx.Observable;

/**
 * {@link RegistryEndpointResolver} defines a common interface for resolving registry endpoints.
 */
public interface RegistryEndpointResolver extends RxRestClient.EndpointResolver {
    /**
     * Returns an observable URI that represents a registry endpoint to connect to.
     */
    Observable<URI> resolve();
}
