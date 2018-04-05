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

import com.netflix.fenzo.TaskRequest;

/**
 * {@link TaskMigrator} is responsible for migrating tasks once an agent is deactivated. The actual migration of each
 * task is handled by a {@link TaskMigrationManager}.
 */
public interface TaskMigrator {

    /**
     * Migrates the the collection of task requests.
     *
     * @param taskRequests the task requests from Fenzo
     */
    void migrate(Collection<TaskRequest> taskRequests);

}
