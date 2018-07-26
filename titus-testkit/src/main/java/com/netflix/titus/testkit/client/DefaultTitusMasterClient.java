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

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClient.TypeProvider;
import com.netflix.titus.common.network.client.RxRestClients;
import com.netflix.titus.common.network.client.TypeProviders;
import com.netflix.titus.runtime.endpoint.common.rest.ErrorResponse;
import rx.Observable;

/**
 * {@link RxRestClient} based {@link TitusMasterClient} implementation.
 */
public class DefaultTitusMasterClient implements TitusMasterClient {

    private static final TypeProvider<ErrorResponse> ERROR_RESPONSE_TP = TypeProviders.of(ErrorResponse.class);

    private static final TypeProvider<List<ApplicationSlaRepresentation>> APPLICATION_SLA_LIST_TP = TypeProviders.of(new TypeReference<List<ApplicationSlaRepresentation>>() {
    });

    private final RxRestClient restClient;

    public DefaultTitusMasterClient(String hostName, int port) {
        this.restClient = RxRestClients.newBuilder("perfClient", new DefaultRegistry())
                .host(hostName)
                .port(port)
                .errorReplyTypeResolver(e -> ERROR_RESPONSE_TP)
                .build();
    }

    @Override
    public Observable<String> addApplicationSLA(ApplicationSlaRepresentation applicationSLA) {
        return restClient.doPOST(
                "/api/v2/management/applications", applicationSLA, TypeProviders.ofEmptyResponse()
        ).flatMap(response -> {
            if (response.getStatusCode() != 201) {
                return Observable.error(new IOException("Errored with HTTP status code " + response.getStatusCode()));
            }
            List<String> locationHeader = response.getHeaders().get("Location");
            if (locationHeader == null) {
                return Observable.error(new IOException("Location header not found in response"));
            }
            return Observable.just(locationHeader.get(0));
        });
    }

    @Override
    public Observable<List<ApplicationSlaRepresentation>> findAllApplicationSLA() {
        return restClient.doGET("/api/v2/management/applications", APPLICATION_SLA_LIST_TP);
    }
}
