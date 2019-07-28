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
package com.netflix.titus.ext.aws.appscale;

import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class AWSAppAutoScalingUtilTest {
    @Test
    public void testGatewayResourceId() {
        final String gatewayResourceId = AWSAppAutoScalingUtil.buildGatewayResourceId(
                "job1", "Titus", "us-east-1", "stage1");
        assertThat(gatewayResourceId).isEqualTo("https://Titus.execute-api.us-east-1.amazonaws.com/stage1/scalableTargetDimensions/job1");
    }

    @Test
    public void testPolicyName() {
        final String policyName = AWSAppAutoScalingUtil.buildScalingPolicyName("policy1", "job1");
        assertThat(policyName).isEqualTo("job1/policy1");
    }
}
