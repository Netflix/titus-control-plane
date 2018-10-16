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
