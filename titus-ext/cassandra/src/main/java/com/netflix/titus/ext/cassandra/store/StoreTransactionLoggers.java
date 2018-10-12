package com.netflix.titus.ext.cassandra.store;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.ext.cassandra.util.StoreTransactionLogger;

public class StoreTransactionLoggers {

    private static final StoreTransactionLogger INSTANCE = StoreTransactionLogger.newBuilder()
            // Job
            .withEntityKeySelectors(Job.class, Job::getId)
            .withEntityFormatter(Job.class, StoreTransactionLoggers::toSummary)

            // Task
            .withEntityKeySelectors(BatchJobTask.class, BatchJobTask::getId)
            .withEntityKeySelectors(ServiceJobTask.class, ServiceJobTask::getId)
            .withEntityFormatter(BatchJobTask.class, StoreTransactionLoggers::toSummary)
            .withEntityFormatter(ServiceJobTask.class, StoreTransactionLoggers::toSummary)

            .build();

    public static StoreTransactionLogger transactionLogger() {
        return INSTANCE;
    }

    private static String toSummary(Job job) {
        if (job == null) {
            return "<null>";
        }
        return "{state=" + job.getStatus().getState() + '}';
    }

    private static String toSummary(Task task) {
        if (task == null) {
            return "<null>";
        }
        return "{state=" + task.getStatus().getState() + '}';
    }
}
