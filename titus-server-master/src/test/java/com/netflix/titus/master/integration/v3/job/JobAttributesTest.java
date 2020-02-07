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

package com.netflix.titus.master.integration.v3.job;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;

@Category(IntegrationTest.class)
public class JobAttributesTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2));

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testIgnoreTitusAttributesFromInput() throws Exception {
        JobDescriptor<ServiceJobExt> job = oneTaskServiceJobDescriptor().but(jd ->
                jd.toBuilder().withAttributes(CollectionsExt.copyAndAdd(
                        jd.getAttributes(),
                        ImmutableMap.<String, String>builder()
                                .put("titus.sanitization.something", "foo")
                                .put("titus.runtimePrediction.anything", "bar")
                                .build()
                ))
        );
        jobsScenarioBuilder.schedule(job, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .assertJob(j -> {
                    Map<String, String> attributes = ((JobDescriptor<?>) j.getJobDescriptor()).getAttributes();
                    return !attributes.containsKey("titus.sanitization.something") &&
                            !attributes.containsKey("titus.runtimePrediction.anything");
                })
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testUpdateJobAttributes() throws Exception {
        JobDescriptor<ServiceJobExt> job = oneTaskServiceJobDescriptor();

        jobsScenarioBuilder.schedule(job, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .updateJobAttributes(Collections.singletonMap("a", "1"))
        );
    }

    @Test(timeout = TEST_TIMEOUT_MS)
    public void testDeleteJobAttributes() throws Exception {
        JobDescriptor<ServiceJobExt> job = oneTaskServiceJobDescriptor();

        jobsScenarioBuilder.schedule(job, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.jobAccepted())
                .updateJobAttributes(Collections.singletonMap("a", "1"))
                .deleteJobAttributes(Collections.singletonList("a"))
        );
    }
}
