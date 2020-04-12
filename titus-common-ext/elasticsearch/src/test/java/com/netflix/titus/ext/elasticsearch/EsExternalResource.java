/*
 * Copyright 2020 Netflix, Inc.
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
package com.netflix.titus.ext.elasticsearch;

import com.google.common.base.Preconditions;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;

public class EsExternalResource extends ExternalResource {
    private static final Logger logger = LoggerFactory.getLogger(EsExternalResource.class);

    private EsClientConfiguration esClientConfiguration;

    @Override
    protected void before() throws Throwable {
        String esHostName = Preconditions.checkNotNull(System.getenv("ES_HOST_NAME"),
                "'ES_HOST_NAME' environment variable not set"
        );

        String esPortStr = Preconditions.checkNotNull(System.getenv("ES_PORT"),
                "'ES_PORT' environment variable not set"
        );

        int esPort = Integer.parseInt(esPortStr);

        // check if a ES cluster state is Green
        WebClient webClient = WebClient.builder()
                .baseUrl(String.format("http://%s:%d", esHostName, esPort)).build();
        String resp = webClient.get()
                .uri("/_cat/health")
                .retrieve()
                .bodyToMono(String.class).block();

        if (resp == null || !resp.contains("green")) {
            throw new IllegalStateException(String.format("Elastic search cluster %s:%d not READY", esHostName, esPort));
        }

        buildEsClientConfiguration(esHostName, esPort);
    }

    public EsClientConfiguration getEsClientConfiguration() {
        return esClientConfiguration;
    }

    private void buildEsClientConfiguration(String esHostName, int esPort) {
        esClientConfiguration = new EsClientConfiguration() {
            @Override
            public int getReadTimeoutSeconds() {
                return 20;
            }

            @Override
            public int getConnectTimeoutMillis() {
                return 1000;
            }

            @Override
            public String getEsHostName() {
                return esHostName;
            }

            @Override
            public int getEsPort() {
                return esPort;
            }
        };
    }
}
