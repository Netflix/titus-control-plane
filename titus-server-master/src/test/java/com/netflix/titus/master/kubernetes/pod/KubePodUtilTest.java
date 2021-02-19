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

package com.netflix.titus.master.kubernetes.pod;

import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;

public class KubePodUtilTest {

    @Test
    public void testCreatePodAnnotationsFromJobParameters() {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = JobFunctions.appendContainerAttribute(job, JobAttributes.JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID, "myAccount");
        job = JobFunctions.appendContainerAttribute(job, JobAttributes.JOB_CONTAINER_ATTRIBUTE_SUBNETS, "subnet1,subnet2");
        assertThat(KubePodUtil.createPodAnnotationsFromJobParameters(job)).containsEntry(KubeConstants.POD_LABEL_ACCOUNT_ID, "myAccount");
        assertThat(KubePodUtil.createPodAnnotationsFromJobParameters(job)).containsEntry(KubeConstants.POD_LABEL_SUBNETS, "subnet1,subnet2");
    }

}