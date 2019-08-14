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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import org.junit.rules.ExternalResource;

public class K8SExternalResource extends ExternalResource {

    public static final Duration K8S_TIMEOUT = Duration.ofSeconds(500);

    private ApiClient client;

    @Override
    protected void before() throws Throwable {
        String k8Server = Preconditions.checkNotNull(System.getenv("K8S_API_SERVER"),
                "'K8S_API_SERVER' environment variable not set"
        );

        this.client = ClientBuilder
                .standard()
                .setBasePath(String.format("http://%s:7001", k8Server))
                .build();
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
    }

    public ApiClient getClient() {
        return client;
    }
}
