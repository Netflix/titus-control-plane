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

package com.netflix.titus.common.util.loadshedding.tokenbucket;

import java.util.Collections;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import org.junit.Test;

import static com.netflix.titus.common.util.loadshedding.tokenbucket.TokenBucketTestConfigurations.DEFAULT_SHARED_CONFIGURATION;
import static com.netflix.titus.common.util.loadshedding.tokenbucket.TokenBucketTestConfigurations.NOT_SHARED_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

public class TokenBucketAdmissionControllerTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @Test
    public void testSharedBucket() {
        TokenBucketAdmissionController controller = new TokenBucketAdmissionController(
                Collections.singletonList(DEFAULT_SHARED_CONFIGURATION),
                titusRuntime
        );

        AdmissionControllerRequest request = AdmissionControllerRequest.newBuilder()
                .withCallerId("any")
                .withEndpointName("any")
                .build();

        // We assume the loop below will complete in a sec so we account for single refill only.
        int limit = DEFAULT_SHARED_CONFIGURATION.getCapacity() + DEFAULT_SHARED_CONFIGURATION.getRefillRateInSec() + 1;

        int stoppedAt = 0;
        while (stoppedAt < limit) {
            AdmissionControllerResponse response = controller.apply(request);
            if (!response.isAllowed()) {
                break;
            }
            stoppedAt++;
        }

        assertThat(stoppedAt).isGreaterThanOrEqualTo(DEFAULT_SHARED_CONFIGURATION.getCapacity());
        assertThat(stoppedAt).isLessThan(limit);
    }

    @Test
    public void testNotSharedBucket() {
        TokenBucketAdmissionController controller = new TokenBucketAdmissionController(
                Collections.singletonList(NOT_SHARED_CONFIGURATION),
                titusRuntime
        );

        AdmissionControllerRequest user1Request = AdmissionControllerRequest.newBuilder()
                .withCallerId("myUser1")
                .withEndpointName("any")
                .build();

        AdmissionControllerRequest user2Request = AdmissionControllerRequest.newBuilder()
                .withCallerId("myUser2")
                .withEndpointName("any")
                .build();

        // We assume the loop below will complete in a sec so we account for single refill only.
        int limit = NOT_SHARED_CONFIGURATION.getCapacity() + NOT_SHARED_CONFIGURATION.getRefillRateInSec() + 1;

        int stoppedAt = 0;
        while (stoppedAt < limit) {
            AdmissionControllerResponse response1 = controller.apply(user1Request);
            AdmissionControllerResponse response2 = controller.apply(user2Request);
            if (response1.isAllowed() && response2.isAllowed()) {
                stoppedAt++;
            } else {
                break;
            }
        }

        assertThat(stoppedAt).isGreaterThanOrEqualTo(NOT_SHARED_CONFIGURATION.getCapacity());
        assertThat(stoppedAt).isLessThan(limit);
    }

    @Test
    public void testNoMatch() {
        TokenBucketAdmissionController controller = new TokenBucketAdmissionController(Collections.emptyList(), titusRuntime);

        AdmissionControllerRequest request = AdmissionControllerRequest.newBuilder()
                .withCallerId("any")
                .withEndpointName("any")
                .build();

        AdmissionControllerResponse response = controller.apply(request);
        assertThat(response.isAllowed()).isTrue();
        assertThat(response.getReasonMessage()).isEqualTo("Rate limits not configured");
    }
}