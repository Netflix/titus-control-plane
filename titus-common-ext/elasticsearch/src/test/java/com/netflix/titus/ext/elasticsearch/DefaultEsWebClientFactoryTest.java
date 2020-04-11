package com.netflix.titus.ext.elasticsearch;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultEsWebClientFactoryTest {
    private static final Logger logger = LoggerFactory.getLogger(DefaultEsWebClientFactoryTest.class);


    private EsClientConfiguration getClientConfiguration() {
        EsClientConfiguration mockConfig = mock(EsClientConfiguration.class);
        when(mockConfig.getEsHostName()).thenReturn("localhost");
        when(mockConfig.getEsPort()).thenReturn(9200);
        return mockConfig;
    }

    @Test
    public void verifyEsHost() {
        final DefaultEsWebClientFactory defaultEsWebClientFactory = new DefaultEsWebClientFactory(getClientConfiguration());
        String esUri = defaultEsWebClientFactory.buildEsUrl();
        assertThat(esUri).isEqualTo("http://localhost:9200");
    }
}
