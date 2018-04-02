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

package com.netflix.titus.master.taskmigration.job;

import java.util.Collection;

import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;
import com.netflix.titus.master.taskmigration.TaskMigrationDetails;
import com.netflix.titus.master.taskmigration.TaskMigrationManager;

import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Pending;
import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Running;
import static com.netflix.titus.master.taskmigration.TaskMigrationManager.State.Skipped;

public class DelayTaskMigrationManager implements TaskMigrationManager {

    private State state;
    private long waitTimeMs;
    private long createTime;


    public DelayTaskMigrationManager(long waitTimeMs) {
        this.state = State.Pending;
        this.waitTimeMs = waitTimeMs;
        this.createTime = System.currentTimeMillis();
    }

    @Override
    public void update(Collection<TaskMigrationDetails> taskMigrationDetailsCollection) {
        if (state == State.Pending) {
            state = State.Running;
        }

        if (hasDurationElasped(createTime, waitTimeMs)) {
            state = State.Skipped;
        }

    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public long getTimeoutMs() {
        return waitTimeMs;
    }

    private boolean hasDurationElasped(long lastTime, long duration) {
        return System.currentTimeMillis() - lastTime > duration;
    }
}
