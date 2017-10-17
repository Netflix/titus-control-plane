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

package io.netflix.titus.master.jobmanager.service.common;

import io.netflix.titus.api.jobmanager.model.job.JobStatus;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;

import static io.netflix.titus.common.util.StringExt.isNotEmpty;

/**
 */
public final class JobFormatters {

    public static String format(JobStatus status) {
        StringBuilder statusBuilder = new StringBuilder(status.getState().toString());
        if (isNotEmpty(status.getReasonCode())) {
            statusBuilder.append('(').append(status.getReasonCode()).append(')');
        }
        return statusBuilder.toString();
    }

    public static String format(TaskStatus status) {
        StringBuilder statusBuilder = new StringBuilder(status.getState().toString());
        if (isNotEmpty(status.getReasonCode())) {
            statusBuilder.append('(').append(status.getReasonCode()).append(')');
        }
        return statusBuilder.toString();
    }
}
