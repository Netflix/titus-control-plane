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
package com.netflix.titus.es.publish;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.es.publish.config.EsPublisherConfiguration;
import com.netflix.titus.ext.elasticsearch.TaskDocument;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Java6Assertions.assertThat;

@Category(IntegrationTest.class)
public class EsClientIntegrationTest {
    private static final String TEST_CONTAINER_NAME = "esClientTest";

    private static final Map<String, String> JobIdToTaskIdMap = new HashMap<String, String>(5) {{
        put("49079946-dd16-4bbf-82a8-9b4e2b7728ce", "e937edd1-4c48-4862-a1cb-6c967d6013e5");
        put("66261b43-76b9-443e-af94-8e7929551399", "b3524aad-20af-4e37-95fa-10376aeae01c");
        put("e9a7561e-7463-4027-b21f-a44da5e579f1", "a803ff8c-8d9a-4d3b-b937-51fed8f19452");
        put("75df0af3-d896-45af-87a6-21d3bfce5480", "7c10a5e0-2811-4d4a-9db2-08d883b5c766");
        put("c71addfb-4021-4ef1-8e30-0ffd33460d31", "eb657a8a-c97b-4ab6-8b67-2ab4585a082c");
    }};

    @Before
    public void setup() throws Exception {
        TestUtils.runLocalDockerCmd(TestUtils.buildDockerRunCmd(TEST_CONTAINER_NAME));
        TestUtils.waitForLocalElasticServerReadiness(10);
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.runLocalDockerCmd(TestUtils.buildDockerStopCmd(TEST_CONTAINER_NAME));
    }

    @Test
    public void indexSingleDocument() throws Exception {
        final EsPublisherConfiguration esPublisherConfiguration = TestUtils.buildEsPublisherConfiguration();
        final TitusClientUtils titusClientUtils = new TitusClientUtils(esPublisherConfiguration);
        final TitusClientImpl titusClient = new TitusClientImpl(TestUtils.buildTitusGrpcChannel(titusClientUtils), new DefaultRegistry());
        final EsClientHttp esClientHttp = new EsClientHttp(TestUtils.buildEsPublisherConfiguration());

        for (Map.Entry<String, String> jobToTask : JobIdToTaskIdMap.entrySet()) {
            final Job job = titusClient.getJobById(jobToTask.getKey()).block();
            final Task task = titusClient.getTask(jobToTask.getValue()).block();
            final com.netflix.titus.api.jobmanager.model.job.Job coreJob = V3GrpcModelConverters.toCoreJob(job);
            final com.netflix.titus.api.jobmanager.model.job.Task coreTask = V3GrpcModelConverters.toCoreTask(coreJob, task);

            final TaskDocument taskDocument = TaskDocument.fromV3Task(coreTask, coreJob,
                    ElasticSearchUtils.dateFormat,
                    ElasticSearchUtils.buildContextMapForTaskDocument("test", "test", "mainvpc"));

            final EsClient.EsIndexResp esIndexResp = esClientHttp.indexTaskDocument(taskDocument).block();
            assertThat(esIndexResp).isNotNull();
            assertThat(esIndexResp.created).isTrue();
            assertThat(esIndexResp._id).isEqualTo(taskDocument.getId());
            assertThat(esIndexResp.result).isEqualTo("created");
        }
        Thread.sleep(1000);
        TestUtils.verifyLocalEsRecordCount(esClientHttp, JobIdToTaskIdMap.size());
    }

    @Test
    public void bulkIndexMultipleDocuments() throws Exception {
        final EsPublisherConfiguration esPublisherConfiguration = TestUtils.buildEsPublisherConfiguration();
        final TitusClientUtils titusClientUtils = new TitusClientUtils(esPublisherConfiguration);
        final TitusClientImpl titusClient = new TitusClientImpl(TestUtils.buildTitusGrpcChannel(titusClientUtils), new DefaultRegistry());
        final EsClientHttp esClientHttp = new EsClientHttp(TestUtils.buildEsPublisherConfiguration());

        final List<TaskDocument> taskDocuments = JobIdToTaskIdMap.keySet().stream()
                .map(jobId -> {
                    final Job job = titusClient.getJobById(jobId).block();
                    final Task task = titusClient.getTask(JobIdToTaskIdMap.get(jobId)).block();
                    final com.netflix.titus.api.jobmanager.model.job.Job coreJob = V3GrpcModelConverters.toCoreJob(job);
                    final com.netflix.titus.api.jobmanager.model.job.Task coreTask = V3GrpcModelConverters.toCoreTask(coreJob, task);

                    return TaskDocument.fromV3Task(coreTask, coreJob,
                            ElasticSearchUtils.dateFormat,
                            ElasticSearchUtils.buildContextMapForTaskDocument("test", "test", "mainvpc"));
                }).collect(Collectors.toList());

        final EsClient.BulkEsIndexResp bulkEsIndexResp = esClientHttp.bulkIndexTaskDocument(taskDocuments).block();
        assertThat(bulkEsIndexResp).isNotNull();
        assertThat(bulkEsIndexResp.items).isNotNull();
        assertThat(bulkEsIndexResp.items).isNotEmpty();
        assertThat(bulkEsIndexResp.items.size()).isEqualTo(JobIdToTaskIdMap.size());
        final List<EsClient.BulkEsIndexRespItem> createdItems = bulkEsIndexResp.items.stream().filter(bulkEsIndexRespItem -> bulkEsIndexRespItem.index.created).collect(Collectors.toList());
        assertThat(createdItems.size()).isEqualTo(JobIdToTaskIdMap.size());

        Thread.sleep(1000);
        TestUtils.verifyLocalEsRecordCount(esClientHttp, JobIdToTaskIdMap.size());
    }


}
