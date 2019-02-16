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

package com.netflix.titus.testkit.client;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

import com.netflix.titus.api.endpoint.v2.rest.representation.ApplicationSlaRepresentation;
import com.netflix.titus.common.util.rx.ReactorExt;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import rx.Observable;

public class ReactorTitusMasterClient implements TitusMasterClient {

    private static final ParameterizedTypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_LIST_TP =
            new ParameterizedTypeReference<List<ApplicationSlaRepresentation>>() {
            };

    private final WebClient client;

    public ReactorTitusMasterClient(String hostName, int port) {
        this.client = WebClient.builder()
                .baseUrl(String.format("http://%s:%s", hostName, port))
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .build();
    }

    @Override
    public Observable<String> addApplicationSLA(ApplicationSlaRepresentation applicationSLA) {
        Mono<String> x = client.post()
                .uri("/api/v2/management/applications")
                .body(BodyInserters.fromObject(applicationSLA))
                .exchange()
                .flatMap(response -> response.toEntity(String.class))
                .flatMap(response -> {
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        return Mono.error(new IOException("Errored with HTTP status code " + response.getStatusCode()));
                    }
                    List<String> locationHeader = response.getHeaders().getOrDefault("Location", Collections.emptyList());
                    if (locationHeader.isEmpty()) {
                        return Mono.error(new IOException("Location header not found in response"));
                    }
                    return Mono.just(locationHeader.get(0));
                });
        return ReactorExt.toObservable(x);
    }

    @Override
    public Observable<List<ApplicationSlaRepresentation>> findAllApplicationSLA() {
        Mono<List<ApplicationSlaRepresentation>> x = client.get()
                .uri("/api/v2/management/applications")
                .retrieve()
                .bodyToMono(APPLICATION_SLA_LIST_TP);
        return ReactorExt.toObservable(x);
    }
}
