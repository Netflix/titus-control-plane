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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public final class JobSnapshotFactories {

    private static final LegacyJobSnapshotFactory LEGACY_JOB_SNAPSHOT_FACTORY = new LegacyJobSnapshotFactory();

    private static final PCollectionJobSnapshotFactory PCOLLECTION_JOB_SNAPSHOT_FACTORY = new PCollectionJobSnapshotFactory(
            false,
            message -> {
            }
    );

    public static JobSnapshotFactory newLegacy() {
        return LEGACY_JOB_SNAPSHOT_FACTORY;
    }

    /**
     * Default {@link JobSnapshotFactory} throws an exception if inconsistent state is detected.
     * Use {@link #newDefault(boolean, Consumer)} to change this behavior.
     */
    public static JobSnapshotFactory newDefault() {
        return PCOLLECTION_JOB_SNAPSHOT_FACTORY;
    }

    /**
     * Returns empty snapshot built using {@link #newDefault()}.
     */
    public static JobSnapshot newDefaultEmptySnapshot() {
        return newDefault().newSnapshot(Collections.emptyMap(), Collections.emptyMap());
    }

    public static JobSnapshotFactory newDefault(boolean autoFixInconsistencies, Consumer<String> inconsistentDataListener) {
        return new PCollectionJobSnapshotFactory(autoFixInconsistencies, inconsistentDataListener);
    }

    private static class LegacyJobSnapshotFactory implements JobSnapshotFactory {

        @Override
        public JobSnapshot newSnapshot(Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
            return LegacyJobSnapshot.newInstance(UUID.randomUUID().toString(), jobsById, tasksByJobId);
        }
    }

    private static class PCollectionJobSnapshotFactory implements JobSnapshotFactory {

        private final boolean autoFixInconsistencies;
        private final Consumer<String> inconsistentDataListener;

        private PCollectionJobSnapshotFactory(boolean autoFixInconsistencies, Consumer<String> inconsistentDataListener) {
            this.autoFixInconsistencies = autoFixInconsistencies;
            this.inconsistentDataListener = inconsistentDataListener;
        }

        @Override
        public JobSnapshot newSnapshot(Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
            return PCollectionJobSnapshot.newInstance(
                    UUID.randomUUID().toString(),
                    jobsById,
                    tasksByJobId,
                    autoFixInconsistencies,
                    inconsistentDataListener
            );
        }
    }
}
