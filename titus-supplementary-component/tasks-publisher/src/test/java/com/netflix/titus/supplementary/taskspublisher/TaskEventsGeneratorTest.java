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
package com.netflix.titus.supplementary.taskspublisher;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.ext.elasticsearch.EsClient;
import com.netflix.titus.ext.elasticsearch.model.BulkEsIndexResp;
import com.netflix.titus.ext.elasticsearch.model.BulkEsIndexRespItem;
import com.netflix.titus.ext.elasticsearch.model.EsIndexResp;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobManagementModelConverters;
import com.netflix.titus.supplementary.taskspublisher.TitusClient.JobOrTaskUpdate;
import com.netflix.titus.supplementary.taskspublisher.config.EsPublisherConfiguration;
import com.netflix.titus.supplementary.taskspublisher.es.EsPublisher;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TaskEventsGeneratorTest {

    private TitusClient mockTitusClient(int numTasks) {
        TitusClient titusClient = mock(TitusClient.class);
        when(titusClient.getJobAndTaskUpdates()).thenReturn(Flux.fromStream(TestUtils.generateSampleTasks(numTasks).stream().map(JobOrTaskUpdate::taskUpdate)));
        when(titusClient.getTask(anyString())).thenReturn(Mono.just(GrpcJobManagementModelConverters.toGrpcTask(JobGenerator.oneBatchTask(), new EmptyLogStorageInfo<>())));
        when(titusClient.getJobById(anyString())).thenReturn(Mono.just(GrpcJobManagementModelConverters.toGrpcJob(JobGenerator.oneBatchJob())));
        return titusClient;
    }

    private EsClient<TaskDocument> mockElasticSearchClient() {
        EsClient<TaskDocument> esClient = mock(EsClient.class);
        when(esClient.bulkIndexDocuments(anyList(), anyString(), anyString())).thenAnswer((Answer<Mono<BulkEsIndexResp>>) invocation -> {
            final List<TaskDocument> documents = invocation.getArgument(0);
            final List<BulkEsIndexRespItem> bulkEsIndexRespItemList = documents.stream().map(doc -> {
                final BulkEsIndexRespItem bulkEsIndexRespItem = new BulkEsIndexRespItem();
                bulkEsIndexRespItem.setIndex(new EsIndexResp());
                bulkEsIndexRespItem.getIndex().setCreated(true);
                bulkEsIndexRespItem.getIndex().setResult("created");
                bulkEsIndexRespItem.getIndex().setId(doc.getId());
                return bulkEsIndexRespItem;
            }).collect(Collectors.toList());

            final BulkEsIndexResp bulkEsIndexResp = new BulkEsIndexResp();
            bulkEsIndexResp.setItems(bulkEsIndexRespItemList);
            return Mono.just(bulkEsIndexResp);
        });
        return esClient;
    }

    private EsPublisherConfiguration mockEsPublisherConfiguration() {
        EsPublisherConfiguration mockConfig = mock(EsPublisherConfiguration.class);
        when(mockConfig.getTaskDocumentEsIndexName()).thenReturn("tasks_");
        when(mockConfig.getTaskDocumentEsIndexDateSuffixPattern()).thenReturn("yyyyMM");
        return mockConfig;
    }

    @Test
    public void checkPublisherState() {
        int numTasks = 5;
        final TaskEventsGenerator taskEventsGenerator = new TaskEventsGenerator(
                mockTitusClient(numTasks),
                Collections.emptyMap());

        EsPublisher esPublisher = new EsPublisher(taskEventsGenerator, mockElasticSearchClient(),
                mockEsPublisherConfiguration(), new DefaultRegistry());
        esPublisher.activate();

        final CountDownLatch latch = new CountDownLatch(1);
        Flux.interval(Duration.ofSeconds(1), Schedulers.elastic())
                .take(1)
                .doOnNext(i -> {
                    final int numTasksUpdated = esPublisher.getNumTasksPublished();
                    final int numErrors = esPublisher.getNumErrorsInPublishing();
                    assertThat(numErrors).isEqualTo(0);
                    assertThat(numTasksUpdated).isGreaterThanOrEqualTo(numTasks);
                    latch.countDown();
                }).subscribe();
        try {
            latch.await(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            fail("Timeout in checkPublisherState ", e);
        }
    }
}