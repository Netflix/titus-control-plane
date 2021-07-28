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

package com.netflix.titus.runtime.connector.jobmanager;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public final class JobSnapshotFactories {

    private static final LegacyJobSnapshotFactory LEGACY_JOB_SNAPSHOT_FACTORY = new LegacyJobSnapshotFactory();
    private static final PCollectionJobSnapshotFactory PCOLLECTION_JOB_SNAPSHOT_FACTORY = new PCollectionJobSnapshotFactory();

    public static JobSnapshotFactory newLegacy() {
        return LEGACY_JOB_SNAPSHOT_FACTORY;
    }

    public static JobSnapshotFactory newDefault() {
        return PCOLLECTION_JOB_SNAPSHOT_FACTORY;
    }

    private static class LegacyJobSnapshotFactory implements JobSnapshotFactory {

        @Override
        public JobSnapshot newSnapshot(Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
            return LegacyJobSnapshot.newInstance(UUID.randomUUID().toString(), jobsById, tasksByJobId);
        }
    }

    private static class PCollectionJobSnapshotFactory implements JobSnapshotFactory {

        @Override
        public JobSnapshot newSnapshot(Map<String, Job<?>> jobsById, Map<String, List<Task>> tasksByJobId) {
            return PCollectionJobSnapshot.newInstance(UUID.randomUUID().toString(), jobsById, tasksByJobId);
        }
    }
}
