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

package com.netflix.titus.master.jobmanager.service;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.Version;
import com.netflix.titus.common.util.time.Clock;

public class VersionSuppliers {

    public static VersionSupplier newInstance(Clock clock) {
        return new SimpleVersionSupplier(clock);
    }

    public static <E extends JobDescriptor.JobDescriptorExt> Job<E> nextVersion(Job<E> job, VersionSupplier supplier) {
        return job.toBuilder()
                .withVersion(supplier.nextVersion())
                .build();
    }

    public static <T extends Task> T nextVersion(T task, VersionSupplier supplier) {
        return (T) task.toBuilder().withVersion(supplier.nextVersion()).build();
    }

    private static class SimpleVersionSupplier implements VersionSupplier {

        private final Clock clock;

        private SimpleVersionSupplier(Clock clock) {
            this.clock = clock;
        }

        @Override
        public Version nextVersion() {
            return Version.newBuilder().withTimestamp(clock.wallTime()).build();
        }
    }
}
