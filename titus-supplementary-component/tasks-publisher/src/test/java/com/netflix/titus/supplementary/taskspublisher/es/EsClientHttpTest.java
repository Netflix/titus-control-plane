package com.netflix.titus.supplementary.taskspublisher.es;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.ext.elasticsearch.DefaultEsClient;
import com.netflix.titus.ext.elasticsearch.DefaultEsWebClientFactory;
import com.netflix.titus.ext.elasticsearch.EsClient;
import com.netflix.titus.ext.elasticsearch.EsClientConfiguration;
import com.netflix.titus.supplementary.taskspublisher.TaskDocument;
import com.netflix.titus.supplementary.taskspublisher.config.EsPublisherConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsClientHttpTest {
    private EsPublisherConfiguration getEsPublisherConfiguration() {
        final EsPublisherConfiguration esPublisherConfiguration = mock(EsPublisherConfiguration.class);
        when(esPublisherConfiguration.getTaskDocumentEsIndexDateSuffixPattern()).thenReturn("yyyyMM");
        when(esPublisherConfiguration.getTaskDocumentEsIndexName()).thenReturn("titustasks_");
        return esPublisherConfiguration;
    }

    private EsClientConfiguration getClientConfiguration() {
        EsClientConfiguration mockConfig = mock(EsClientConfiguration.class);
        when(mockConfig.getEsHostName()).thenReturn("localhost");
        when(mockConfig.getEsPort()).thenReturn(9200);
        return mockConfig;
    }

    @Test
    public void verifyBulkIndexPayload() {
        final HashMap<Job, BatchJobTask> jobToTaskMap = new HashMap<>();
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());

        final List<TaskDocument> taskDocuments = jobToTaskMap.keySet().stream()
                .map(job -> TaskDocument.fromV3Task(jobToTaskMap.get(job), job, ElasticSearchUtils.DATE_FORMAT,
                        Collections.emptyMap()))
                .collect(Collectors.toList());

        DefaultEsWebClientFactory defaultEsWebClientFactory = new DefaultEsWebClientFactory(getClientConfiguration());
        EsClient<TaskDocument> esClient = new DefaultEsClient<>(defaultEsWebClientFactory);
        final String bulkIndexPayload = esClient.buildBulkIndexPayload(taskDocuments, "titustasks");
        assertThat(bulkIndexPayload).isNotNull();
        assertThat(bulkIndexPayload).isNotEmpty();
        final String[] payloadLines = bulkIndexPayload.split("\n");
        assertThat(payloadLines.length).isEqualTo(jobToTaskMap.size() * 2);
        assertThat(payloadLines[0]).contains("index");
        assertThat(payloadLines[2]).contains("index");
        assertThat(payloadLines[4]).contains("index");
    }
}