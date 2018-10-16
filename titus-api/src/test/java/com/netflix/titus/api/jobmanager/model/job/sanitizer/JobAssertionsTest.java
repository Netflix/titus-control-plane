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

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.model.ResourceDimension;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JobAssertionsTest {

    @Test
    public void testImageDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:abcdef0123456789abcdef0123456789abcdef0123456789")
                .build();
        Map<String, String> violations = new JobAssertions(id -> ResourceDimension.empty()).validateImage(image);
        assertThat(violations).isEmpty();
    }

    @Test
    public void testInvalidDigestValidation() {
        Image image = Image.newBuilder()
                .withName("imageName")
                .withDigest("sha256:XYZ")
                .build();
        Map<String, String> violations = new JobAssertions(id -> ResourceDimension.empty()).validateImage(image);
        assertThat(violations).hasSize(1);
    }
}