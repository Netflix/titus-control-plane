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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.model.job.JobGenerator;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import org.junit.jupiter.api.Test;

import static com.netflix.titus.master.kubernetes.pod.KubePodConstants.POD_SCHEMA_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RouterPodFactoryTest {

    private final KubePodConfiguration configuration = mock(KubePodConfiguration.class);

    @Test
    void testRoutesToV1Pod() {
        when(configuration.getDefaultPodSpecVersion()).thenReturn("v0");
        String routingRules = "v1=(testApp1);v2=(testApp2)";
        List<Pair<String, String>> versionMappings = Arrays.asList(Pair.of("v0", "0"), Pair.of("v1", "1"));
        RouterPodFactory routerPodFactory = createRouterPodFactory(routingRules, versionMappings);

        Job<BatchJobExt> job = createBatchJob("testApp3", "testCG3");
        BatchJobTask task = JobGenerator.oneBatchTask();
        V1Pod v1Pod = routerPodFactory.buildV1Pod(job, task);
        assertThat(v1Pod.getMetadata().getAnnotations().get(POD_SCHEMA_VERSION)).isEqualTo("0");
    }

    @Test
    void testRoutesToDefault() {
        String routingRules = "v1=(testApp1);v2=(testApp2)";
        List<Pair<String, String>> versionMappings = Arrays.asList(Pair.of("v0", "0"), Pair.of("v1", "1"));
        RouterPodFactory routerPodFactory = createRouterPodFactory(routingRules, versionMappings);

        Job<BatchJobExt> job = createBatchJob("testApp1", "testCG1");
        BatchJobTask task = JobGenerator.oneBatchTask();
        V1Pod v1Pod = routerPodFactory.buildV1Pod(job, task);
        assertThat(v1Pod.getMetadata().getAnnotations().get(POD_SCHEMA_VERSION)).isEqualTo("1");
    }

    @Test
    void testExtractPatterns() {
        Map<String, Pattern> stringPatternMap = RouterPodFactory.extractPatterns("v1=(a|b|c);v2=(app2)");
        assertThat(stringPatternMap).hasSize(2);
        Pattern v1 = stringPatternMap.get("v1");
        assertThat(v1).isNotNull();
        assertThat(v1.pattern()).isEqualTo("(a|b|c)");
    }

    @Test
    void testEmptyExtractPatterns() {
        assertThat(RouterPodFactory.extractPatterns("")).hasSize(0);
        assertThat(RouterPodFactory.extractPatterns(null)).hasSize(0);
    }

    private V1Pod createPodWithVersion(String version) {
        V1ObjectMeta metadata = new V1ObjectMeta()
                .annotations(Collections.singletonMap(POD_SCHEMA_VERSION, version));
        return new V1Pod().metadata(metadata);
    }

    private Job<BatchJobExt> createBatchJob(String applicationName, String capacityGroup) {
        return JobGenerator.oneBatchJob().but(
                job -> job.toBuilder()
                        .withJobDescriptor(job.getJobDescriptor().toBuilder()
                                .withApplicationName(applicationName)
                                .withCapacityGroup(capacityGroup)
                                .build())
                        .build()
        );
    }

    private RouterPodFactory createRouterPodFactory(String routingRules, List<Pair<String, String>> versionMappings) {
        when(configuration.getPodSpecVersionRoutingRules()).thenReturn(routingRules);
        Map<String, PodFactory> versionedPodFactories = new LinkedHashMap<>();

        for (Pair<String, String> versionMapping : versionMappings) {
            PodFactory podFactory = mock(PodFactory.class);
            when(podFactory.buildV1Pod(any(), any())).thenReturn(createPodWithVersion(versionMapping.getRight()));
            versionedPodFactories.put(versionMapping.getLeft(), podFactory);
        }

        return new RouterPodFactory(configuration, versionedPodFactories);
    }
}