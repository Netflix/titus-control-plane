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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.net.ssl.SSLException;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.network.client.NettySslContextEngineFactory;
import com.netflix.titus.common.network.client.RxHttpResponse;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClientException;
import com.netflix.titus.common.network.client.RxRestClients;
import com.netflix.titus.common.network.client.TypeProviders;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Single;

/**
 * This {@link DefaultDockerRegistryClient} implementation of {@link RegistryClient} connects to a
 * Docker V2 REST API compatible registry endpoint.
 */
@Singleton
@ProxyConfiguration(types = {ProxyType.Logging, ProxyType.Spectator})
public class DefaultDockerRegistryClient implements RegistryClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDockerRegistryClient.class);

    private static final String acceptHeader = "Accept";
    private static final String dockerManifestType = "application/vnd.docker.distribution.manifest.v2+json";
    private static final String dockerDigestHeaderKey = "Docker-Content-Digest";

    private static final Map<String, String> headers = Collections.unmodifiableMap(
            Stream.of(new AbstractMap.SimpleEntry<>(acceptHeader, dockerManifestType))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    private final TitusRegistryClientConfiguration titusRegistryClientConfiguration;
    private final RxRestClient restClient;

    @Inject
    DefaultDockerRegistryClient(RegistryEndpointResolver endpointResolver, TitusRegistryClientConfiguration configuration, Registry spectatorRegistry) {
        this.titusRegistryClientConfiguration = configuration;

        RxRestClients.Builder builder = RxRestClients.newBuilder("dockerRegistryClient", spectatorRegistry)
                .endpointResolver(endpointResolver)
                // We rely on the RxRestClient for timeout handling
                .timeUnit(TimeUnit.MILLISECONDS)
                .requestTimeout(titusRegistryClientConfiguration.getRegistryTimeoutMs())
                .retryCount(titusRegistryClientConfiguration.getRegistryRetryCount())
                .retryDelay(titusRegistryClientConfiguration.getRegistryRetryDelayMs())
                .noRetryStatuses(Collections.singleton(HttpResponseStatus.NOT_FOUND));
        if (titusRegistryClientConfiguration.isSecure()) {
            try {
                builder.sslEngineFactory(new NettySslContextEngineFactory(SslContextBuilder.forClient().build()));
            } catch (SSLException e) {
                logger.error("Unable configure Docker registry client SSL context: {}", e);
                throw new RuntimeException("Error configuring SSL context", e);
            }
        }
        this.restClient = builder.build();
    }

    /**
     * Gets the Docker Version 2 Schema 2 Content Digest for the provided repository and reference. The
     * reference may be an image tag or digest value. If the image does not exist or another error is
     * encountered, an onError value is emitted.
     */
    public Single<String> getImageDigest(String repository, String reference) {
        return registryRequestWithErrorHandling(
                restClient.doGET(buildRegistryUri(repository, reference), headers, TypeProviders.ofEmptyResponse()), repository, reference)
                .map(RxHttpResponse::getHeaders)
                .flatMap(stringListMap -> {
                    if (stringListMap.containsKey(dockerDigestHeaderKey)) {
                        return Observable.from(stringListMap.get(dockerDigestHeaderKey));
                    }
                    return Observable.error(new TitusRegistryException(TitusRegistryException.ErrorCode.MISSING_HEADER, "Missing required header " + dockerDigestHeaderKey));
                }).first().toSingle();
    }

    private String buildRegistryUri(String repository, String reference) {
        return "/v2/" + repository + "/manifests/" + reference;
    }

    /**
     * Wraps an observable registry request with timeouts and error handling.
     */
    private <T> Observable<T> registryRequestWithErrorHandling(Observable<T> obs, String repository, String reference) {
        return obs.timeout(titusRegistryClientConfiguration.getRegistryTimeoutMs(), TimeUnit.MILLISECONDS)
                .onErrorResumeNext(throwable -> {
                    if (throwable instanceof RxRestClientException) {
                        if (((RxRestClientException)throwable).getStatusCode() == HttpResponseStatus.NOT_FOUND.code()) {
                            return Observable.error(
                                    new TitusRegistryException(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND,
                                    String.format("Image %s:%s does not exist in registry", repository, reference)));
                        }
                    }
                    return  Observable.error(
                            new TitusRegistryException(TitusRegistryException.ErrorCode.INTERNAL,
                                    throwable.getMessage()));
                });
    }
}
