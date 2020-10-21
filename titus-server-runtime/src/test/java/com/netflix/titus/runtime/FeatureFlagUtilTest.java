/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FeatureFlagUtilTest {

    @Test
    public void testIsNoKubeSchedulerMigration() {
        assertThat(FeatureFlagUtil.isNoKubeSchedulerMigration(JobDescriptorGenerator.oneTaskBatchJobDescriptor())).isFalse();
        assertThat(FeatureFlagUtil.isNoKubeSchedulerMigration(newJob(false))).isFalse();
        assertThat(FeatureFlagUtil.isNoKubeSchedulerMigration(newJob(true))).isTrue();
    }

    private JobDescriptor<?> newJob(boolean allow) {
        return JobFunctions.appendJobDescriptorAttribute(
                JobDescriptorGenerator.oneTaskBatchJobDescriptor(),
                JobAttributes.JOB_PARAMETER_NO_KUBE_SCHEDULER_MIGRATION, Boolean.toString(allow)
        );
    }
}