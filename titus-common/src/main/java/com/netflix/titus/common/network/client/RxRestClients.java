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

package com.netflix.titus.common.network.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.network.client.internal.RetryableRestClient;
import com.netflix.titus.common.network.client.internal.RxClientMetric;
import com.netflix.titus.common.network.client.internal.RxNettyRestClient;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.pipeline.ssl.SSLEngineFactory;
import rx.Observable;
import rx.schedulers.Schedulers;

public class RxRestClients {

    public static Builder newBuilder(String name, Registry registry) {
        return new Builder(name, registry);
    }

    private static class SingleHostEndpointResolver implements RxRestClient.EndpointResolver {

        private final Observable<URI> resolveValue;

        private SingleHostEndpointResolver(String host, int port, boolean secure) {
            String scheme = secure ? "https" : "http";
            String uriStr = scheme + "://" + host + ':' + port;
            try {
                this.resolveValue = Observable.just(new URI(uriStr));
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URI: " + uriStr);
            }
        }

        @Override
        public Observable<URI> resolve() {
            return resolveValue;
        }
    }

    public static class Builder {
        private final String name;
        private final Registry registry;

        private String host;
        private int port;
        private SSLEngineFactory sslEngineFactory;
        private RxRestClient.EndpointResolver endpointResolver;
        private Function<Integer, RxRestClient.TypeProvider<?>> errorReplyTypeResolver;
        private int retryCount;
        private long requestTimeout;
        private long retryDelay;
        private TimeUnit timeUnit;
        private Set<HttpResponseStatus> noRetryStatuses;
        private RxRestClientConfiguration configuration;


        public Builder(String name, Registry registry) {
            this.name = name;
            this.registry = registry;
        }

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder sslEngineFactory(SSLEngineFactory sslEngineFactory) {
            this.sslEngineFactory = sslEngineFactory;
            return this;
        }

        public Builder endpointResolver(RxRestClient.EndpointResolver endpointResolver) {
            this.endpointResolver = endpointResolver;
            return this;
        }

        public Builder errorReplyTypeResolver(Function<Integer, RxRestClient.TypeProvider<?>> errorReplyTypeResolver) {
            this.errorReplyTypeResolver = errorReplyTypeResolver;
            return this;
        }

        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder requestTimeout(long requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public Builder retryDelay(long retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder noRetryStatuses(Set<HttpResponseStatus> statuses) {
            this.noRetryStatuses = statuses;
            return this;
        }

        public Builder configuration(RxRestClientConfiguration configuration) {
            this.configuration = configuration;
            return this;
        }

        public RxRestClient build() {

            RxClientMetric clientMetric = new RxClientMetric(name, registry);
            RxRestClient.EndpointResolver resolver = getEndpointResolver();

            RxRestClient rxRestClient = new RxNettyRestClient(resolver, clientMetric, Optional.ofNullable(errorReplyTypeResolver),
                    Optional.ofNullable(sslEngineFactory));

            if (configuration != null) {
                retryCount = configuration.getRetryCount();
                requestTimeout = configuration.getRequestTimeoutMs();
                retryDelay = configuration.getRetryDelayMs();
                timeUnit = TimeUnit.MILLISECONDS;
                noRetryStatuses = Collections.emptySet();
            }

            if (retryCount > 0) {
                rxRestClient = new RetryableRestClient(rxRestClient, retryCount, requestTimeout, retryDelay, timeUnit, noRetryStatuses, clientMetric, Schedulers.computation());
            }

            return rxRestClient;
        }

        private RxRestClient.EndpointResolver getEndpointResolver() {

            if (endpointResolver != null) {
                return endpointResolver;
            }

            Preconditions.checkArgument(!Strings.isNullOrEmpty(host), "Host must be valid");
            boolean secure = !Objects.isNull(sslEngineFactory);
            return new SingleHostEndpointResolver(host, port, secure);
        }
    }
}
