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

package io.netflix.titus.master.taskmigration;

public interface TaskMigrationDetails {
    String getId();

    String getJobId();

    long getCreatedTimeMillis();

    int getNumberOfInstances();

    boolean isService();

    String getApplicationName();

    String getStack();

    String getDetail();

    String getSequence();

    String getImageName();

    String getImageVersion();

    String getAttribute(String key);

    boolean isActive();

    long getMigrationDeadline();

    void setMigrationDeadline(long migrationDeadline);
}
