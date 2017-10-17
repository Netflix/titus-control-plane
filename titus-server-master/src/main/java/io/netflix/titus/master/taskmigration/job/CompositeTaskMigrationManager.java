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

package io.netflix.titus.master.taskmigration.job;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Preconditions;
import io.netflix.titus.master.taskmigration.TaskMigrationDetails;
import io.netflix.titus.master.taskmigration.TaskMigrationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeTaskMigrationManager implements TaskMigrationManager {

    private final static Logger logger = LoggerFactory.getLogger(CompositeTaskMigrationManager.class);

    private final Queue<TaskMigrationManager> migrationManagers;
    private final long timeoutMs;
    private TaskMigrationManager currentMigrationManager;
    private State state;

    public CompositeTaskMigrationManager(List<TaskMigrationManager> migrationManagers) {
        Preconditions.checkArgument(!migrationManagers.isEmpty(), "migrationManagers cannot be empty");
        this.migrationManagers = new ConcurrentLinkedQueue<>(migrationManagers);
        this.timeoutMs = migrationManagers.stream().mapToLong(TaskMigrationManager::getTimeoutMs).sum();

        state = State.Pending;
    }

    @Override
    public void update(Collection<TaskMigrationDetails> taskMigrationDetailsCollection) {

        if (state == State.Pending) {
            state = State.Running;
        }

        if (currentMigrationManager == null) {
            if (migrationManagers.isEmpty()) {
                state = State.Failed;
                logger.warn("Went through all migration managers so failing.");
                return;
            } else {
                currentMigrationManager = migrationManagers.poll();
            }
        }

        if (currentMigrationManager.getState() == State.Failed || currentMigrationManager.getState() == State.Skipped) {
            currentMigrationManager = null;
        } else if (currentMigrationManager.getState() == State.Succeeded) {
            state = State.Succeeded;
        } else {
            currentMigrationManager.update(taskMigrationDetailsCollection);
        }
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public long getTimeoutMs() {
        return timeoutMs;
    }
}
