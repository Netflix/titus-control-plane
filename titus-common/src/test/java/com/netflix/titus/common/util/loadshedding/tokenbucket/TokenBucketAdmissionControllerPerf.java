/*
 * Copyright 2022 Netflix, Inc.
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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllers;

public class TokenBucketAdmissionControllerPerf {

    private static final TitusRuntime titusRuntime = TitusRuntimes.internal();

    public static void main(String[] args) {
        long durationSec = 60;
        TokenBucketAdmissionController controller = new TokenBucketAdmissionController(Arrays.asList(
                newTokenBucketConfiguration("bucket1"),
                newTokenBucketConfiguration("bucket2")
        ), AdmissionControllers.noBackoff(), false, titusRuntime);

        Stopwatch stopwatch = Stopwatch.createStarted();
        AdmissionControllerRequest bucket1Request = AdmissionControllerRequest.newBuilder()
                .withCallerId("bucket1")
                .withEndpointName("bucket1")
                .build();
        AdmissionControllerRequest bucket2Request = AdmissionControllerRequest.newBuilder()
                .withCallerId("bucket2")
                .withEndpointName("bucket2")
                .build();
        long allowed1 = 0;
        long rejected1 = 0;
        long allowed2 = 0;
        long rejected2 = 0;
        while (stopwatch.elapsed(TimeUnit.SECONDS) < durationSec) {
            if (controller.apply(bucket1Request).isAllowed()) {
                allowed1++;
            } else {
                rejected1++;
            }
            if (controller.apply(bucket2Request).isAllowed()) {
                allowed2++;
            } else {
                rejected2++;
            }
        }
        System.out.println("Allowed1  / sec: " + (allowed1 / durationSec));
        System.out.println("Rejected1 / sec: " + (rejected1 / durationSec));
        System.out.println("Allowed2  / sec: " + (allowed2 / durationSec));
        System.out.println("Rejected2 / sec: " + (rejected2 / durationSec));
    }

    private static TokenBucketConfiguration newTokenBucketConfiguration(String id) {
        return new TokenBucketConfiguration(
                id,
                1,
                true,
                id + ".*",
                id + ".*",
                2000,
                1000
        );
    }
}
