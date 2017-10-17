/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.perf.load.catalog;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;

/**
 * Set of predefined job profiles.
 */
public final class JobCatalog {
    public enum JobSize {
        Small, Medium, Large
    }

    private static AtomicInteger idx = new AtomicInteger();

    private JobCatalog() {
    }

    public static TitusJobSpec batchJob(JobSize jobSize, int instances, long duration, TimeUnit timeUnit) {
        int factor = 1;
        switch (jobSize) {
            case Small:
                factor = 1;
                break;
            case Medium:
                factor = 4;
                break;
            case Large:
                factor = 10;
                break;
        }
        double cpu = factor;
        double memory = 512 * factor;
        double disk = 64 * factor;
        double network = 64 * factor;

        return new TitusJobSpec.Builder()
                .type(TitusJobType.batch)
                .appName("titusLoadTestBatch")
                .name("titusLoadTest" + idx.getAndIncrement())
                .applicationName("trustybase")
                .instances(instances)
                .cpu(cpu)
                .memory(memory)
                .disk(disk)
                .networkMbps(network)
                .user("titusLoadTest")
                .entryPoint("sleep " + timeUnit.toSeconds(duration))
                .build();
    }

    public static TitusJobSpec serviceJob(JobSize jobSize, int min, int desired, int max) {
        int factor = 1;
        switch (jobSize) {
            case Small:
                factor = 1;
                break;
            case Medium:
                factor = 2;
                break;
            case Large:
                factor = 8;
                break;
        }
        double cpu = factor;
        double memory = 512 * factor;
        double disk = 64 * factor;
        double network = 64 * factor;

        return new TitusJobSpec.Builder()
                .type(TitusJobType.service)
                .appName("titusLoadTestService")
                .name("titusLoadTest" + idx.getAndIncrement())
                .applicationName("trustybase")
                .instancesMin(min)
                .instancesDesired(desired)
                .instancesMax(max)
                .cpu(cpu)
                .memory(memory)
                .disk(disk)
                .networkMbps(network)
                .user("titusLoadTest")
                .entryPoint("sleep 36000")
                .build();
    }
}
