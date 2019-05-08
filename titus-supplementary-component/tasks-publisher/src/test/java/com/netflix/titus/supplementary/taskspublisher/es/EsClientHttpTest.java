package com.netflix.titus.supplementary.taskspublisher.es;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.supplementary.taskspublisher.TaskDocument;
import com.netflix.titus.supplementary.taskspublisher.config.EsPublisherConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsClientHttpTest {
    private EsPublisherConfiguration getConfig() {
        final EsPublisherConfiguration esPublisherConfiguration = mock(EsPublisherConfiguration.class);
        when(esPublisherConfiguration.getEsHostName()).thenReturn("localhost");
        when(esPublisherConfiguration.getEsPort()).thenReturn(9200);
        when(esPublisherConfiguration.getTaskDocumentEsIndexDateSuffixPattern()).thenReturn("yyyyMM");
        when(esPublisherConfiguration.getTaskDocumentEsIndexName()).thenReturn("titustasks_");
        return esPublisherConfiguration;
    }

    @Test
    public void verifyEsHost() {
        final DefaultEsWebClientFactory defaultEsWebClientFactory = new DefaultEsWebClientFactory(getConfig());
        String esUri = defaultEsWebClientFactory.buildEsUrl();
        assertThat(esUri).isEqualTo("http://localhost:9200");
    }


    @Test
    public void verifyCurrentEsIndexName() {
        String monthlySuffix = new SimpleDateFormat("yyyyMM").format(new Date());

        final EsClientHttp esClientHttp = new EsClientHttp(getConfig(), new DefaultEsWebClientFactory(getConfig()));
        final String esIndexNameCurrent = esClientHttp.buildEsIndexNameCurrent();
        assertThat(esIndexNameCurrent).isNotNull();
        assertThat(esIndexNameCurrent).isNotEmpty();
        assertThat(esIndexNameCurrent).isEqualTo(String.format("titustasks_%s", monthlySuffix));
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

        final EsClientHttp esClientHttp = new EsClientHttp(getConfig(), new DefaultEsWebClientFactory(getConfig()));
        final String bulkIndexPayload = esClientHttp.buildBulkIndexPayload(taskDocuments, "titustasks");
        assertThat(bulkIndexPayload).isNotNull();
        assertThat(bulkIndexPayload).isNotEmpty();
        final String[] payloadLines = bulkIndexPayload.split("\n");
        assertThat(payloadLines.length).isEqualTo(jobToTaskMap.size() * 2);
        assertThat(payloadLines[0]).contains("index");
        assertThat(payloadLines[2]).contains("index");
        assertThat(payloadLines[4]).contains("index");
    }
}