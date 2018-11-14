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

package com.netflix.titus.master.eviction.service;


import com.netflix.titus.api.jobmanager.model.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EvictionTransactionLog {

    private static final Logger logger = LoggerFactory.getLogger("EvictionTransactionLog");

    void logTaskTermination(Job<?> job, String taskId, String reason, String callerId) {
        doLog(job.getId(), taskId, reason, callerId, "success", "Task terminated");
    }

    void logTaskTerminationError(Job<?> job, String taskId, String reason, String callerId, Throwable error) {
        doLog(job.getId(), taskId, reason, callerId, "error", error.getMessage());
    }

    void logTaskTerminationUnexpectedError(String taskId, String reason, String callerId, Throwable error) {
        doLog("?", taskId, reason, callerId, "error", error.getMessage());
    }

    private static void doLog(String jobId,
                              String taskId,
                              String reason,
                              String callerId,
                              String status,
                              String summary) {
        String message = String.format(
                "jobId=%s taskId=%s caller=%-15s status=%-7s reason=%-35s summary=%s",
                jobId,
                taskId,
                callerId,
                status,
                reason,
                summary
        );
        logger.info(message);
    }
}
