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

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.network.client.TitusWebClientAddOns;
import com.netflix.titus.common.network.client.WebClientExt;
import com.netflix.titus.common.network.client.internal.WebClientMetric;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;


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

    private final TitusRuntime titusRuntime;
    private final TitusRegistryClientConfiguration titusRegistryClientConfiguration;
    private final WebClient restClient;
    private final WebClientMetric webClientMetrics;

    @Inject
    DefaultDockerRegistryClient(TitusRegistryClientConfiguration configuration, TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        this.titusRegistryClientConfiguration = configuration;
        this.webClientMetrics = new WebClientMetric(DefaultDockerRegistryClient.class.getSimpleName(), titusRuntime.getRegistry());

        this.restClient = WebClient.builder()
                .baseUrl(configuration.getRegistryUri())
                .apply(b -> TitusWebClientAddOns.addTitusDefaults(b, titusRegistryClientConfiguration.isSecure(), webClientMetrics))
                .build();
    }

    /**
     * Gets the Docker Version 2 Schema 2 Content Digest for the provided repository and reference. The
     * reference may be an image tag or digest value. If the image does not exist or another error is
     * encountered, an onError value is emitted.
     */
    public Mono<String> getImageDigest(String repository, String reference) {
        return restClient.get().uri(buildRegistryUri(repository, reference))
                .headers(consumer -> headers.forEach(consumer::add))
                .exchange()
                .flatMap(response -> response.toEntity(String.class))
                .flatMap(response -> {
                    if (response.getStatusCode().value() == HttpResponseStatus.NOT_FOUND.code()) {
                        return Mono.error(
                                TitusRegistryException.imageNotFound(repository, reference)
                        );
                    }
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        return Mono.error(
                                TitusRegistryException.internalError(repository, reference, response.getStatusCode())
                        );
                    }
                    HttpHeaders responseHeaders = response.getHeaders();
                    List<String> dockerDigestHeaderValue = responseHeaders.getOrDefault(dockerDigestHeaderKey, Collections.emptyList());
                    if (dockerDigestHeaderValue.isEmpty()) {
                        return Mono.error(
                                TitusRegistryException.headerMissing(repository, reference, dockerDigestHeaderKey)
                        );
                    }
                    return Mono.just(dockerDigestHeaderValue.get(0));
                })
                .timeout(Duration.ofMillis(titusRegistryClientConfiguration.getRegistryTimeoutMs()))
                .transformDeferred(WebClientExt.latencyMonoOperator(titusRuntime.getClock(), webClientMetrics, HttpMethod.GET))
                .retryWhen(TitusWebClientAddOns.retryer(
                        Duration.ofMillis(titusRegistryClientConfiguration.getRegistryRetryDelayMs()),
                        titusRegistryClientConfiguration.getRegistryRetryCount(),
                        error -> !(error instanceof TitusRegistryException),
                        logger
                ));
    }

    private String buildRegistryUri(String repository, String reference) {
        return "/v2/" + repository + "/manifests/" + reference;
    }
}
