package io.netflix.titus.ext.cassandra.tool;

import java.util.List;

import io.netflix.titus.common.util.CollectionsExt;

import static java.util.Arrays.asList;

public class CassandraSchemas {

    public static final String ACTIVE_JOB_IDS_TABLE = "active_job_ids";
    public static final String ACTIVE_JOB_TABLE = "active_jobs";
    public static final String ACTIVE_TASK_IDS_TABLE = "active_task_ids";
    public static final String ACTIVE_TASKS_TABLE = "active_tasks";

    public static final String ARCHIVED_JOBS_TABLE = "archived_jobs";
    public static final String ARCHIVED_TASK_IDS_TABLE = "archived_task_ids";
    public static final String ARCHIVED_TASKS_TABLE = "archived_tasks";

    public static final List<String> JOB_ACTIVE_TABLES = asList(
            ACTIVE_JOB_IDS_TABLE, ACTIVE_JOB_TABLE, ACTIVE_TASK_IDS_TABLE, ACTIVE_TASKS_TABLE
    );

    public static final List<String> JOB_ARCHIVE_TABLES = asList(
            ARCHIVED_JOBS_TABLE, ARCHIVED_TASK_IDS_TABLE, ARCHIVED_TASKS_TABLE
    );

    public static final List<String> JOB_TABLES = CollectionsExt.merge(JOB_ACTIVE_TABLES, JOB_ARCHIVE_TABLES);
}
