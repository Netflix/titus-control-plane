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

package com.netflix.titus.api.jobmanager.model.job.sanitizer;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.model.ResourceDimension;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class JobAssertionsTest {

    private final JobConfiguration configuration = mock(JobConfiguration.class);

    private final JobAssertions jobAssertions = new JobAssertions(configuration, id -> ResourceDimension.empty());

    @Test
    public void testEmptyMapOfEnvironmentVariableNames() {
        Map<String, String> violations = jobAssertions.validateEnvironmentVariableNames(Collections.emptyMap());
        assertThat(violations).isEmpty();
    }

    @Test
    public void testGoodEnvironmentVariableNames() {
        Map<String, String> violations = jobAssertions.validateEnvironmentVariableNames(ImmutableMap.of(
                "THIS_IS_GOOD_123", "good name",
                "_THIS_IS_2_GOOD", "good name",
                "_AND_THIS", "good name"
        ));
        assertThat(violations).isEmpty();
    }

    @Test
    public void testBadEnvironmentVariableNames() {
        assertThat(jobAssertions.validateEnvironmentVariableNames(ImmutableMap.of(
                "THIS_IS_GOOD_123", "good name",
                "this is bad", "bad value",
                "1_THIS_IS_NOT_GOOD", "bad name",
                "", "bad value"
        )).keySet()).containsExactlyInAnyOrder("empty", "invalidFirstCharacter", "invalidCharacter");

        assertThat(jobAssertions.validateEnvironmentVariableNames(ImmutableMap.of(
                "This_is_bad", "bad value",
                "and_this_is_bad", "bad value"
        )).keySet()).containsExactlyInAnyOrder("invalidFirstCharacter", "invalidCharacter");
    }

    @Test
    public void testImageDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:abcdef0123456789abcdef0123456789abcdef0123456789")
                .build();
        Map<String, String> violations = jobAssertions.validateImage(image);
        assertThat(violations).isEmpty();
    }

    @Test
    public void testInvalidDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:XYZ")
                .build();
        Map<String, String> violations = jobAssertions.validateImage(image);
        assertThat(violations).hasSize(1);
    }
}