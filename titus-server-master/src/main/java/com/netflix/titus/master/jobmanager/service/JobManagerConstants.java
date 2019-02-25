package com.netflix.titus.master.jobmanager.service;

import com.netflix.titus.api.jobmanager.model.CallMetadata;

/**
 * Constant keys for Job Manager attributes.
 */
public class JobManagerConstants {
    /**
     * Call metadata attribute for a job/task mutation
     */
    public static final String JOB_MANAGER_ATTRIBUTE_CALLMETADATA = "callmetadata";

    /**
     * Call metadata for a reconciler job/task mutation
     */
    public static final CallMetadata RECONCILER_CALLMETADATA = CallMetadata.newBuilder().withCallerId("Reconciler").build();

    /**
     * Call metadata for a scheduler job/task mutation
     */
    public static final CallMetadata SCHEDULER_CALLMETADATA = CallMetadata.newBuilder().withCallerId("Scheduler").build();

    /**
     * Call metadata for a task migrator job/task mutation
     */
    public static final CallMetadata TASK_MIGRATOR_CALLMETADATA = CallMetadata.newBuilder().withCallerId("TaskMigrator").build();

    /**
     * Call metadata for an undefined job/task mutation
     */
    public static final CallMetadata UNDEFINED_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Unknown").withCallReason("Unknown").build();
}