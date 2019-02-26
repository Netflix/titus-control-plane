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

import com.netflix.titus.testkit.junit.category.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;


@Category(IntegrationTest.class)
public class TasksPublisherCtrlTest {
    private static final String TEST_CONTAINER_NAME = "taskPublisherTest";

    @Before
    public void setup() throws Exception {
        TestUtils.runLocalDockerCmd(TestUtils.buildDockerRunCmd(TEST_CONTAINER_NAME));
        TestUtils.waitForLocalElasticServerReadiness(10);
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.runLocalDockerCmd(TestUtils.buildDockerStopCmd(TEST_CONTAINER_NAME));
    }

    /*
    @Test
    public void verifyEndToEndWorkflow() {
        final EsPublisherConfiguration esPublisherConfiguration = TestUtils.buildEsPublisherConfiguration();
        final TitusClientUtils titusClientUtils = new TitusClientUtils(esPublisherConfiguration);
        final TitusClientImpl titusClient = new TitusClientImpl(TestUtils.buildTitusGrpcChannel(titusClientUtils), new DefaultRegistry());
        final EsClientHttp esClientHttp = new EsClientHttp(TestUtils.buildEsPublisherConfiguration());

        final TasksPublisherCtrl tasksPublisherCtrl = new TasksPublisherCtrl(esClientHttp, titusClient, titusClientUtils,
                new DefaultRegistry());
        tasksPublisherCtrl.start();

        final CountDownLatch latch = new CountDownLatch(1);
        Flux.interval(Duration.ofSeconds(10), Schedulers.elastic())
                .take(1)
                .doOnNext(i -> {
                    final int numTimesIndexUpdated = tasksPublisherCtrl.getNumIndexUpdated().get();
                    final int numTasksUpdated = tasksPublisherCtrl.getNumTasksUpdated().get();
                    final int numErrors = tasksPublisherCtrl.getNumErrors().get();

                    assertThat(numErrors).isEqualTo(0);
                    assertThat(numTimesIndexUpdated).isGreaterThanOrEqualTo(numTasksUpdated);
                    TestUtils.verifyLocalEsRecordCount(esClientHttp, numTasksUpdated);

                    latch.countDown();
                }).subscribe();

        try {
            latch.await(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            fail("Timeout in verifyTasksPublished ", e);
        }
    }
    */
}