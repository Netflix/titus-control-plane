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
