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

package com.netflix.titus.runtime.connector.jobmanager.snapshot;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;

public final class JobSnapshotFactories {

    /**
     * Default {@link JobSnapshotFactory} throws an exception if inconsistent state is detected.
     * Use {@link #newDefault(boolean, boolean, Consumer, TitusRuntime)} to change this behavior.
     */
    public static JobSnapshotFactory newDefault(TitusRuntime titusRuntime) {
        return new PCollectionJobSnapshotFactory(
                false,
                false,
                message -> {
                },
                titusRuntime
        );
    }

    /**
     * Returns empty snapshot built using {@link #newDefault(TitusRuntime)}.
     */
    public static JobSnapshot newDefaultEmptySnapshot(TitusRuntime titusRuntime) {
        return newDefault(titusRuntime).newSnapshot(Collections.emptyMap(), Collections.emptyMap());
    }

    public static JobSnapshotFactory newDefault(boolean autoFixInconsistencies,
                                                boolean archiveMode,
                                                Consumer<String> inconsistentDataListener,
                                                TitusRuntime titusRuntime) {
        return new PCollectionJobSnapshotFactory(autoFixInconsistencies, archiveMode, inconsistentDataListener, titusRuntime);
    }

    private static class PCollectionJobSnapshotFactory implements JobSnapshotFactory {

        private final boolean autoFixInconsistencies;
        private final boolean archiveMode;
        private final Consumer<String> inconsistentDataListener;
        private final TitusRuntime titusRuntime;

        private PCollectionJobSnapshotFactory(boolean autoFixInconsistencies,
                                              boolean archiveMode,
                                              Consumer<String> inconsistentDataListener,
                                              TitusRuntime titusRuntime) {
            this.autoFixInconsistencies = autoFixInconsistencies;
            this.archiveMode = archiveMode;
            this.inconsistentDataListener = inconsistentDataListener;
            this.titusRuntime = titusRuntime;
        }

        @Override
        public JobSnapshot newSnapshot(Map<String, Job<?>> jobsById, Map<String, Map<String, Task>> tasksByJobId) {
            return PCollectionJobSnapshot.newInstance(
                    UUID.randomUUID().toString(),
                    jobsById,
                    tasksByJobId,
                    autoFixInconsistencies,
                    archiveMode,
                    inconsistentDataListener,
                    titusRuntime
            );
        }
    }
}
