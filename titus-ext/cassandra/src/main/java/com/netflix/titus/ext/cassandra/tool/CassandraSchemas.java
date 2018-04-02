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

package com.netflix.titus.ext.cassandra.tool;

import java.util.List;

import com.netflix.titus.common.util.CollectionsExt;

import static java.util.Arrays.asList;

public class CassandraSchemas {

    public static final String ACTIVE_JOB_IDS_TABLE = "active_job_ids";
    public static final String ACTIVE_JOBS_TABLE = "active_jobs";
    public static final String ACTIVE_TASK_IDS_TABLE = "active_task_ids";
    public static final String ACTIVE_TASKS_TABLE = "active_tasks";

    public static final String ARCHIVED_JOBS_TABLE = "archived_jobs";
    public static final String ARCHIVED_TASK_IDS_TABLE = "archived_task_ids";
    public static final String ARCHIVED_TASKS_TABLE = "archived_tasks";

    public static final List<String> JOB_ACTIVE_TABLES = asList(
            ACTIVE_JOB_IDS_TABLE, ACTIVE_JOBS_TABLE, ACTIVE_TASK_IDS_TABLE, ACTIVE_TASKS_TABLE
    );

    public static final List<String> JOB_ARCHIVE_TABLES = asList(
            ARCHIVED_JOBS_TABLE, ARCHIVED_TASK_IDS_TABLE, ARCHIVED_TASKS_TABLE
    );

    public static final List<String> JOB_TABLES = CollectionsExt.merge(JOB_ACTIVE_TABLES, JOB_ARCHIVE_TABLES);
}
