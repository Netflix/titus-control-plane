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

package com.netflix.titus.ext.eureka.spring;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;
import javax.ws.rs.core.UriBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.ext.eureka.common.EurekaLoadBalancer;
import com.netflix.titus.ext.eureka.common.EurekaUris;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import reactor.core.publisher.Mono;

import static java.util.Arrays.asList;

public class EurekaLoadBalancingExchangeFilterFunction implements ExchangeFilterFunction {

    private static final Logger logger = LoggerFactory.getLogger(EurekaLoadBalancingExchangeFilterFunction.class);

    private static final String EUREKA_SCHEMA = "eureka";

    private final EurekaLoadBalancer loadBalancer;

    public EurekaLoadBalancingExchangeFilterFunction(EurekaClient eurekaClient,
                                                     Function<URI, String> vipExtractor,
                                                     TitusRuntime titusRuntime) {
        this.loadBalancer = new EurekaLoadBalancer(eurekaClient, vipExtractor, titusRuntime);
    }

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        URI eurekaUri;
        try {
            eurekaUri = EurekaUris.failIfEurekaUriInvalid(request.url());
        } catch (IllegalArgumentException e) {
            logger.warn(e.getMessage());
            logger.debug("Stack trace", e);
            return Mono.just(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage()).build());
        }

        return loadBalancer.chooseNext(eurekaUri)
                .map(instance -> doExecute(instance, request, next))
                .orElseGet(() -> doFailOnNoInstance(eurekaUri));
    }

    private Mono<ClientResponse> doExecute(InstanceInfo instance, ClientRequest request, ExchangeFunction next) {
        URI eurekaUri = request.url();
        URI rewrittenURI = rewrite(eurekaUri, instance);

        ClientRequest newRequest = ClientRequest.create(request.method(), rewrittenURI)
                .headers(headers -> headers.addAll(request.headers()))
                .cookies(cookies -> cookies.addAll(request.cookies()))
                .attributes(attributes -> attributes.putAll(request.attributes()))
                .body(request.body()).build();
        return next.exchange(newRequest)
                .doOnNext(response -> {
                    if (response.statusCode().is5xxServerError()) {
                        loadBalancer.recordFailure(eurekaUri, instance);
                    } else {
                        loadBalancer.recordSuccess(eurekaUri, instance);
                    }
                })
                .doOnError(error -> loadBalancer.recordFailure(eurekaUri, instance));
    }

    private Mono<ClientResponse> doFailOnNoInstance(URI eurekaUri) {
        return Mono.just(ClientResponse.create(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Server pool empty for eurekaUri=" + eurekaUri)
                .build()
        );
    }

    @VisibleForTesting
    static URI rewrite(URI original, InstanceInfo instance) {
        URI effectiveUri;
        if (original.getScheme().equals(EUREKA_SCHEMA)) {
            boolean secure = StringExt.isNotEmpty(original.getQuery()) && asList(original.getQuery().split("&")).contains("secure=true");
            try {
                effectiveUri = new URI((secure ? "https" : "http") + original.toString().substring(EUREKA_SCHEMA.length()));
            } catch (URISyntaxException e) {
                effectiveUri = original;
            }
        } else {
            effectiveUri = original;
        }
        return UriBuilder.fromUri(effectiveUri).host(instance.getIPAddr()).build();
    }
}
