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

package com.netflix.titus.master.taskmigration;

import java.util.Collection;

import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Failed;
import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Skipped;
import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Succeeded;

/**
 * {@link TaskMigrationManager} is responsible for a single migration unit whether it be one task or a group of tasks.
 * Each manager will be orchestrated by a {@link TaskMigrator}.
 */
public interface TaskMigrationManager {

    enum State {
        Pending,
        Running,
        Succeeded,
        Failed,
        Skipped
    }

    /**
     * @param state
     * @return whether or not the state is terminal.
     */
    static boolean isTerminalState(State state) {
        return state == Failed || state == Succeeded || state == Skipped;
    }

    /**
     * Performs the actual migration logic based on a collection of tasks. This method should not block or the execution
     * loop calling it will slow down.
     *
     * @param taskMigrationDetailsCollection
     */
    void update(Collection<TaskMigrationDetails> taskMigrationDetailsCollection);

    State getState();

    /**
     * @return the timeout for this manager
     */
    long getTimeoutMs();
}
