package com.netflix.titus.es.publish;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import com.netflix.titus.ext.elasticsearch.TaskDocument;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.log4j.*;

public class EsClientHttpTest {
    private static final Logger logger = Logger.getLogger(EsTasksPublish.class);
    static {
        logger.setLevel(Level.INFO);
    }


    private EsPublisherConfiguration getConfig(String region) {
        final EsPublisherConfiguration esPublisherConfiguration = mock(EsPublisherConfiguration.class);
        when(esPublisherConfiguration.getRegion()).thenReturn(region);
        when(esPublisherConfiguration.getTaskDocumentEsIndexDateSuffixPattern()).thenReturn("yyyyMM");
        when(esPublisherConfiguration.getTaskDocumentEsIndexName()).thenReturn("titustasks_");
        return esPublisherConfiguration;
    }

    @Test
    public void verifyEsHost() {
        String region = "us-west-2";
        EsClientHttp esClientHttp = new EsClientHttp(getConfig(region));
        String esUri = esClientHttp.buildEsUrl();
        assertThat(esUri).isEqualTo("http://es_titus_tasks_uswest2.us-west-2.dynprod.netflix.net:7104");

        region = "us-east-1";
        esClientHttp = new EsClientHttp(getConfig(region));
        esUri = esClientHttp.buildEsUrl();
        assertThat(esUri).isEqualTo("http://es_titus_tasks_useast1.us-east-1.dynprod.netflix.net:7104");
    }


    @Test
    public void verifyCurrentEsIndexName() {
        String region = "us-west-2";
        String monthlySuffix = new SimpleDateFormat("yyyyMM").format(new Date());

        final EsClientHttp esClientHttp = new EsClientHttp(getConfig(region));
        final String esIndexNameCurrent = esClientHttp.buildEsIndexNameCurrent();
        assertThat(esIndexNameCurrent).isNotNull();
        assertThat(esIndexNameCurrent).isNotEmpty();
        assertThat(esIndexNameCurrent).isEqualTo(String.format("titustasks_uswest2_%s", monthlySuffix));
    }

    @Test
    public void verifyBulkIndexPayload() {
        final HashMap<Job, BatchJobTask> jobToTaskMap = new HashMap<>();
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        jobToTaskMap.put(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());

        final List<TaskDocument> taskDocuments = jobToTaskMap.keySet().stream()
                .map(job -> TaskDocument.fromV3Task(jobToTaskMap.get(job), job, ElasticSearchUtils.dateFormat,
                        Collections.emptyMap()))
                .collect(Collectors.toList());

        final EsClientHttp esClientHttp = new EsClientHttp(getConfig("us-east-1"));
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