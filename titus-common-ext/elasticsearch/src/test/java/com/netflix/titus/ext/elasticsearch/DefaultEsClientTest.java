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

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultEsClientTest {
    private EsClientConfiguration getClientConfiguration() {
        EsClientConfiguration mockConfig = mock(EsClientConfiguration.class);
        when(mockConfig.getEsHostName()).thenReturn("localhost");
        when(mockConfig.getEsPort()).thenReturn(9200);
        return mockConfig;
    }

    @Test
    public void bulkIndexPayload() {
        TestDoc testDoc1 = new TestDoc("id1", "Accepted", Instant.now().getEpochSecond());
        TestDoc testDoc2 = new TestDoc("id2", "Running", Instant.now().getEpochSecond());
        TestDoc testDoc3 = new TestDoc("id3", "Stopped", Instant.now().getEpochSecond());

        List<TestDoc> testDocs = Arrays.asList(testDoc1, testDoc2, testDoc3);

        DefaultEsWebClientFactory defaultEsWebClientFactory = new DefaultEsWebClientFactory(getClientConfiguration());
        DefaultEsClient<TestDoc> esClient = new DefaultEsClient<>(defaultEsWebClientFactory);
        final String bulkIndexPayload = esClient.buildBulkIndexPayload(testDocs, "titustasks", "default");
        assertThat(bulkIndexPayload).isNotNull();
        assertThat(bulkIndexPayload).isNotEmpty();
        final String[] payloadLines = bulkIndexPayload.split("\n");
        assertThat(payloadLines.length).isEqualTo(testDocs.size() * 2);
        assertThat(payloadLines[0]).contains("index");
        assertThat(payloadLines[2]).contains("index");
        assertThat(payloadLines[4]).contains("index");
    }
}
