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

package com.netflix.titus.master.mesos.kubeapiserver.direct.resourcepool;

import java.util.Collections;
import java.util.List;

import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FarzonePodResourcePoolResolverTest {

    private static final String FARZONE_ID = "myFarzone";

    private final DirectKubeConfiguration configuration = Archaius2Ext.newConfiguration(DirectKubeConfiguration.class,
            "titusMaster.directKube.farzones", FARZONE_ID
    );

    private final FarzonePodResourcePoolResolver resolver = new FarzonePodResourcePoolResolver(configuration);

    @Test
    public void testFarzoneJobAssignment() {
        List<ResourcePoolAssignment> result = resolver.resolve(newJob(FARZONE_ID));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo(PodResourcePoolResolvers.RESOURCE_POOL_ELASTIC_FARZONE_PREFIX + FARZONE_ID);
    }

    @Test
    public void testNoneFarzoneJobAssignment() {
        List<ResourcePoolAssignment> result = resolver.resolve(newJob("regularZone"));
        assertThat(result).hasSize(0);
    }

    private Job newJob(String zoneId) {
        Job<BatchJobExt> job = JobGenerator.oneBatchJob();
        job = job.toBuilder()
                .withJobDescriptor(job.getJobDescriptor().toBuilder()
                        .withContainer(job.getJobDescriptor().getContainer().toBuilder()
                                .withHardConstraints(Collections.singletonMap(JobConstraints.AVAILABILITY_ZONE, zoneId))
                                .build()
                        )
                        .build()
                )
                .build();
        return job;
    }

}