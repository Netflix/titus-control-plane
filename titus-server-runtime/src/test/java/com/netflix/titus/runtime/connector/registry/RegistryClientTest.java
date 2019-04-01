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

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

public class RegistryClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private ClientAndServer mockServer;

    private final TitusRegistryClientConfiguration configuration = mock(TitusRegistryClientConfiguration.class);
    private RegistryClient registryClient;

    @Before
    public void setUp() {
        mockServer = startClientAndServer(0);

        when(configuration.getRegistryUri()).thenReturn("http://localhost:" + mockServer.getPort());
        when(configuration.isSecure()).thenReturn(false);
        when(configuration.getRegistryTimeoutMs()).thenReturn(500);
        when(configuration.getRegistryRetryCount()).thenReturn(2);
        when(configuration.getRegistryRetryDelayMs()).thenReturn(5);

        registryClient = new DefaultDockerRegistryClient(configuration, titusRuntime);
    }

    @After
    public void tearDown() {
        mockServer.stop();
    }

    @Test
    public void getDigestTest() {
        final String repo = "titusops/alpine";
        final String tag = "latest";
        final String digest = "sha256:f9f5bb506406b80454a4255b33ed2e4383b9e4a32fb94d6f7e51922704e818fa";

        mockServer
                .when(
                        HttpRequest.request()
                                .withMethod("GET")
                                .withPath("/v2/" + repo + "/manifests/" + tag)
                )
                .respond(
                        HttpResponse.response()
                                .withStatusCode(HttpResponseStatus.OK.code())
                                .withHeader(
                                        new Header("Docker-Content-Digest", digest)
                                )
                                .withBody("{\"schemaVersion\": 2}")
                );

        String retrievedDigest = registryClient.getImageDigest(repo, tag).timeout(TIMEOUT).block();
        assertThat(retrievedDigest).isEqualTo(digest);
    }

    @Test
    public void missingImageTest() {
        final String repo = "titusops/alpine";
        final String tag = "doesnotexist";

        mockServer
                .when(
                        HttpRequest.request().withPath("/v2/" + repo + "/manifests/" + tag)
                ).respond(HttpResponse.response()
                .withStatusCode(HttpResponseStatus.NOT_FOUND.code()));

        try {
            registryClient.getImageDigest(repo, tag).timeout(TIMEOUT).block();
        } catch (TitusRegistryException e) {
            assertThat(e.getErrorCode()).isEqualTo(TitusRegistryException.ErrorCode.IMAGE_NOT_FOUND);
        }
    }
}
