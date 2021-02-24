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

package com.netflix.titus.master.kubernetes.pod.resourcepool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodResourcePoolResolverChainTest {

    private final PodResourcePoolResolver delegate1 = mock(PodResourcePoolResolver.class);
    private final PodResourcePoolResolver delegate2 = mock(PodResourcePoolResolver.class);

    private final PodResourcePoolResolverChain resolver = new PodResourcePoolResolverChain(Arrays.asList(delegate1, delegate2));

    @Test
    public void testChainedExecution() {
        when(delegate1.resolve(any(), any())).thenReturn(Collections.emptyList());
        when(delegate2.resolve(any(), any())).thenReturn(Collections.singletonList(
                ResourcePoolAssignment.newBuilder().withResourcePoolName("elastic").withRule("rule").build())
        );
        List<ResourcePoolAssignment> result = resolver.resolve(JobGenerator.oneBatchJob(), JobGenerator.oneBatchTask());
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getResourcePoolName()).isEqualTo("elastic");
    }
}