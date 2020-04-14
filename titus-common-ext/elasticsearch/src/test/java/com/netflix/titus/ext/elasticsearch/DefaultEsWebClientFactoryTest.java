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

import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultEsWebClientFactoryTest {
    private EsClientConfiguration getClientConfiguration() {
        EsClientConfiguration mockConfig = mock(EsClientConfiguration.class);
        when(mockConfig.getHost()).thenReturn("localhost");
        when(mockConfig.getPort()).thenReturn(9200);
        return mockConfig;
    }

    @Test
    public void verifyEsHost() {
        final DefaultEsWebClientFactory defaultEsWebClientFactory = new DefaultEsWebClientFactory(getClientConfiguration());
        String esUri = defaultEsWebClientFactory.buildEsUrl();
        assertThat(esUri).isEqualTo("http://localhost:9200");
    }
}
