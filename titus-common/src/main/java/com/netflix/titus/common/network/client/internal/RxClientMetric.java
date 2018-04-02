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

package com.netflix.titus.common.network.client.internal;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.network.client.RxRestClientException;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public final class RxClientMetric {

    private final Registry registry;
    private final Id getId;
    private final Id postId;
    private final Id putId;

    public RxClientMetric(String name, Registry registry) {
        this.registry = registry;
        this.getId = registry.createId("titus.rxRestClient", "client", name, "method", "GET");
        this.postId = registry.createId("titus.rxRestClient", "client", name, "method", "POST");
        this.putId = registry.createId("titus.rxRestClient", "client", name, "method", "PUT");
    }

    Id createGetId(String nameSuffix) {
        return registry.createId(getId.name() + '.' + nameSuffix, getId.tags());
    }

    Id createMethodId(HttpMethod method) {
        if (method == HttpMethod.GET) {
            return getId;
        } else if (method == HttpMethod.POST) {
            return postId;
        } else if (method == HttpMethod.PUT) {
            return putId;
        }
        return null;
    }

    void increment(Id requestId) {
        registry.counter(requestId).increment();
    }

    void increment(Id requestId, HttpResponseStatus statusCode) {
        registry.counter(requestId.withTag("statusCode", Integer.toString(statusCode.code()))).increment();
    }

    void increment(Id requestId, Throwable error) {
        registry.counter(createErrorIdBasedOnExceptionContent(requestId, error)).increment();
    }

    void registerLatency(Id id, long latency) {
        Id delayId = registry.createId(id.name() + ".latency", id.tags());
        registry.timer(delayId).record(latency, TimeUnit.MILLISECONDS);
    }

    private Id createErrorIdBasedOnExceptionContent(Id id, Throwable error) {
        Id errorId = id;
        if (error instanceof RxRestClientException) {
            RxRestClientException ce = (RxRestClientException) error;
            if (ce.getStatusCode() > 0) {
                errorId = errorId
                        .withTag("errorType", "http")
                        .withTag("status", Integer.toString(ce.getStatusCode()));
            } else {
                errorId = errorId.withTag("errorType", "connectivity");
            }
        }
        return errorId;
    }
}
