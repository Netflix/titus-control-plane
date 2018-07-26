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

package com.netflix.titus.testkit.client;

import java.util.List;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import rx.Observable;

/**
 * A simple implementation of Titus v2 REST API, which reuses server side REST API data model.
 * It is provided solely for testing purposes. This implementation is provided as a default client by
 * {@link TitusMasterResource}.
 */
public interface TitusMasterClient {

    /*
     * Capacity group management.
     */

    Observable<String> addApplicationSLA(ApplicationSlaRepresentation applicationSLA);

    Observable<List<ApplicationSlaRepresentation>> findAllApplicationSLA();
}
