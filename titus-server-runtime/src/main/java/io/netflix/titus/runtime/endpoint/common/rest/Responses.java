/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.runtime.endpoint.common.rest;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.validation.ConstraintViolationException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import io.netflix.titus.api.service.TitusServiceException;
import rx.Completable;
import rx.Observable;

public class Responses {
    public static <T> List<T> fromObservable(Observable<?> observable) {
        try {
            return (List<T>) observable.timeout(1, TimeUnit.MINUTES).toList().toBlocking().firstOrDefault(null);
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

    public static Response fromVoidObservable(Observable<Void> observable, Response.Status statusCode) {
        fromObservable(observable);
        return Response.status(statusCode).build();
    }

    public static Response fromCompletable(Completable completable) {
        return fromCompletable(completable, Response.Status.OK);
    }

    public static Response fromCompletable(Completable completable, Response.Status statusCode) {
        try {
            completable.await(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw fromException(e);
        }
        return Response.status(statusCode).build();
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
