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

package com.netflix.titus.runtime.endpoint.common.rest;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import com.netflix.titus.api.service.TitusServiceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

public class Responses {

    private static final Duration REST_TIMEOUT_DURATION = Duration.ofMinutes(1);

    private static final long REST_TIMEOUT_DURATION_MS = REST_TIMEOUT_DURATION.toMillis();

    public static <T> List<T> fromObservable(Observable<?> observable) {
        try {
            return (List<T>) observable.timeout(1, TimeUnit.MINUTES).toList().toBlocking().firstOrDefault(null);
        } catch (Exception e) {
            throw fromException(e);
        }
    }

    public static <T> T fromMono(Mono<T> mono) {
        try {
            return mono.timeout(REST_TIMEOUT_DURATION).block();
        } catch (Exception e) {
            throw fromException(e);
        }
    }

    public static Response fromVoidMono(Mono<Void> mono) {
        try {
            mono.timeout(REST_TIMEOUT_DURATION).ignoreElement().block();
            return Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            throw fromException(e);
        }
    }

    public static ResponseEntity<Void> fromVoidMono(Mono<Void> mono, HttpStatus status) {
        try {
            mono.timeout(REST_TIMEOUT_DURATION).ignoreElement().block();
            return ResponseEntity.status(status).build();
        } catch (Exception e) {
            throw fromException(e);
        }
    }

    public static <T> T fromSingleValueObservable(Observable<?> observable) {
        List result = fromObservable(observable);
        if (result.isEmpty()) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return (T) result.get(0);
    }

    public static Response fromCompletable(Completable completable) {
        return fromCompletable(completable, Response.Status.OK);
    }

    public static Response fromCompletable(Completable completable, Response.Status statusCode) {
        try {
            completable.await(REST_TIMEOUT_DURATION_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw fromException(e);
        }
        return Response.status(statusCode).build();
    }

    public static ResponseEntity<Void> fromCompletable(Completable completable, HttpStatus statusCode) {
        try {
            completable.await(REST_TIMEOUT_DURATION_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw fromException(e);
        }
        return ResponseEntity.status(statusCode).build();
    }

    private static RuntimeException fromException(Exception e) {
        if (e instanceof TitusServiceException) {
            return (TitusServiceException) e;
        } else if (e instanceof ConstraintViolationException) {
            return (ConstraintViolationException) e;
        }
        return RestExceptions.from(e);
    }
}
