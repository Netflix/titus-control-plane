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

package com.netflix.titus.master.integration.v3.job;

import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.runtime.TitusEntitySanitizerModule.CUSTOM_JOB_CONFIGURATION_ROOT;
import static com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters.toGrpcJobDescriptor;
import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class JobCustomConfigurationValidatorTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(1)
            .toMaster(master -> master
                    .withProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".pattern", ".*")
                    .withProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".maxBatchJobSize", "1"
                    )
            ));

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder);

    private JobManagementServiceGrpc.JobManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
        client = titusStackResource.getGateway().getV3BlockingGrpcClient();
    }

    @Test
    public void testCustomBatchJobSizeValidation() {
        JobDescriptor<BatchJobExt> jobDescriptor = JobFunctions.changeBatchJobSize(
                oneTaskBatchJobDescriptor().toBuilder()
                        .withApplicationName("a1")
                        .build(),
                2
        );
        DefaultSettableConfig config = titusStackResource.getMaster().getConfig();
        config.setProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".a1.pattern", ".*");
        config.setProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".a1.maxBatchJobSize", "1");

        try {
            client.createJob(toGrpcJobDescriptor(jobDescriptor));
            Assert.fail("Expected to fail due to validation error");
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
    }

    @Test
    public void testCustomServiceJobSizeValidation() {
        JobDescriptor<ServiceJobExt> jobDescriptor = JobFunctions.changeServiceJobCapacity(
                oneTaskServiceJobDescriptor().toBuilder()
                        .withApplicationName("a1")
                        .build(),
                Capacity.newBuilder().withMax(2).build()
        );
        DefaultSettableConfig config = titusStackResource.getMaster().getConfig();
        config.setProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".a1.pattern", ".*");
        config.setProperty(CUSTOM_JOB_CONFIGURATION_ROOT + ".a1.maxServiceJobSize", "1");

        try {
            client.createJob(toGrpcJobDescriptor(jobDescriptor));
            Assert.fail("Expected to fail due to validation error");
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        }
    }
}
