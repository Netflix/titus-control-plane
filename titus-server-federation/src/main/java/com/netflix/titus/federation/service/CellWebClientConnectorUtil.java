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

package com.netflix.titus.federation.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Either;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CellWebClientConnectorUtil {

    /**
     * Run GET operation on all cells and merge the result.
     */
    public static <T> Either<List<T>, WebApplicationException> doGetAndMerge(CellWebClientConnector cellWebClientsSupplier,
                                                                             String path,
                                                                             ParameterizedTypeReference<List<T>> type,
                                                                             long timeoutMs) {
        Either<List<List<T>>, Throwable> partials;
        try {
            partials = Flux
                    .merge(
                            cellWebClientsSupplier.getWebClients().values().stream()
                                    .map(c ->
                                            c.get().uri(path)
                                                    .retrieve()
                                                    .bodyToMono(type)
                                    )
                                    .collect(Collectors.toList())
                    )
                    .collectList()
                    .<Either<List<List<T>>, Throwable>>map(Either::ofValue)
                    .onErrorResume(e -> Mono.just(Either.ofError(e)))
                    .block(Duration.ofMillis(timeoutMs));
        } catch (Exception e) {
            return Either.ofError(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
        }

        if (partials == null) {
            return Either.ofError(new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR));
        }
        if (partials.hasError()) {
            return Either.ofError(new WebApplicationException(partials.getError(), Response.Status.INTERNAL_SERVER_ERROR));
        }
        if (CollectionsExt.isNullOrEmpty(partials.getValue())) {
            return Either.ofValue(Collections.emptyList());
        }

        List<T> result = new ArrayList<>();
        partials.getValue().forEach(result::addAll);
        return Either.ofValue(result);
    }

    /**
     * Run GET operation on all cells, but only one is expected to return a result.
     */
    public static <T> Either<T, WebApplicationException> doGetFromCell(CellWebClientConnector cellWebClientsSupplier,
                                                                       String path,
                                                                       ParameterizedTypeReference<T> type,
                                                                       long timeoutMs) {
        List<Either<T, Throwable>> partials;
        try {
            partials = Flux.merge(
                    cellWebClientsSupplier.getWebClients().values().stream()
                            .map(cell ->
                                    cell.get().uri(path)
                                            .retrieve()
                                            .bodyToMono(type)
                                            .map(Either::<T, Throwable>ofValue)
                                            .onErrorResume(e -> Mono.just(Either.ofError(e)))
                            )
                            .collect(Collectors.toList())
            ).collectList().block(Duration.ofMillis(timeoutMs));
        } catch (Exception e) {
            return Either.ofError(new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR));
        }

        if (CollectionsExt.isNullOrEmpty(partials)) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        Throwable systemError = null;
        for (Either<T, Throwable> partial : partials) {
            if (partial.hasValue()) {
                return Either.ofValue(partial.getValue());
            }
            if (partial.getError() instanceof WebClientResponseException) {
                WebClientResponseException wcError = (WebClientResponseException) partial.getError();
                if (!wcError.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
                    systemError = partial.getError();
                }
            } else {
                systemError = partial.getError();
            }
        }

        // If there is a system error, report it. Otherwise all cells returned NOT_FOUND, and we can send it back to
        // the client.
        return Either.ofError(systemError != null
                ? new WebApplicationException(systemError)
                : new WebApplicationException(Response.Status.NOT_FOUND)
        );
    }
}
