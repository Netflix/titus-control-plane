/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.netflix.titus.api.agent.model.AgentFunctions.withId;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withJobId;
import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroups;
import static com.netflix.titus.testkit.model.agent.AgentTestFunctions.inState;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultDeschedulerServiceTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final MutableDataGenerator<AgentInstanceGroup> flexInstanceGroupGenerator = new MutableDataGenerator<>(agentServerGroups(Tier.Flex, 10));

    private final MutableDataGenerator<Job<ServiceJobExt>> jobGenerator = new MutableDataGenerator<>(
            JobGenerator.serviceJobs(oneTaskServiceJobDescriptor().but(ofServiceSize(4)))
    );

    private final ReschedulerDataGenerator dataGenerator = new ReschedulerDataGenerator(titusRuntime)
            .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId("active1"), inState(InstanceGroupLifecycleState.Active)))
            .addInstanceGroup(flexInstanceGroupGenerator.getValue().but(withId("removable1"), inState(InstanceGroupLifecycleState.Removable)))
            .addJob(jobGenerator.getValue().but(withJobId("job1")))
            .addJob(jobGenerator.getValue().but(withJobId("job2")));

    private final DefaultDeschedulerService deschedulerService = new DefaultDeschedulerService(
            dataGenerator.getJobOperations(),
            dataGenerator.getEvictionOperations(),
            dataGenerator.getAgentOperations(),
            titusRuntime
    );

    @Test
    public void testAllExpectedJobMigrationsAreFound() {
        dataGenerator.place("job1", "active1", 0, 1);
        dataGenerator.place("job1", "removable1", 2, 3);
        dataGenerator.setQuota("job1", 2);

        dataGenerator.place("job2", "active1", 0, 1);
        dataGenerator.place("job2", "removable1", 2, 3);
        dataGenerator.setQuota("job2", 2);

        List<DeschedulingResult> result = deschedulerService.deschedule(Collections.emptyMap());
        assertThat(result).hasSize(4);
    }
}