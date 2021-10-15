/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job.other;

import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.netflix.titus.master.endpoint.MasterEndpointModule.GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX;
import static com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters.basicMasterWithKubeIntegration;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(IntegrationTest.class)
public class JobLoadSheddingTest extends BaseIntegrationTest {

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            basicMasterWithKubeIntegration(EmbeddedKubeClusters.basicCluster(2))
            .toBuilder()
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.order", "1")
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.sharedByCallers", "true")
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.callerPattern", ".*")
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.endpointPattern", ".*")
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.capacity", "1")
            .withProperty(GRPC_ADMISSION_CONTROLLER_CONFIGURATION_PREFIX + ".default.refillRateInSec", "1")
            .build()
    );

    private JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        client = titusMasterResource.getMaster().getV3BlockingGrpcClient();
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testMasterRateLimits() {
        int counter = 0;
        try {
            // Assuming execution time is < 1sec we should fail quickly.
            for (; counter < 100; counter++) {
                client.findTasks(TaskQuery.newBuilder().setPage(Page.newBuilder().setPageSize(1).build()).build());
            }
            fail("Expected rate limit error");
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
        }
        // We expect at least one request to succeed.
        assertThat(counter).isGreaterThan(0);
    }
}
