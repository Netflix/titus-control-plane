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

package io.netflix.titus.api.model.v2;


import io.netflix.titus.api.store.v2.V2WorkerMetadata;

public class WorkerNaming {

    public static class JobWorkerIdPair {
        public String jobId;
        public int workerIndex;
        public int workerNumber;
    }

    private static final String delimiter = "-worker-";

    public static String getTaskId(V2WorkerMetadata task) {
        return getWorkerName(task.getJobId(), task.getWorkerIndex(), task.getWorkerNumber());
    }

    public static String getWorkerName(String jobId, int workerIndex, int workerNumber) {
        return jobId + delimiter + workerIndex + "-" + workerNumber;
    }

    public static boolean isValidForWorkerName(String jobId, int workerIndex, int workerNumber) {
        if (workerIndex < 0 || workerNumber < 0) {
            return false;
        }
        return !(jobId == null || jobId.isEmpty() || jobId.indexOf('-') < 0);
    }

    public static JobWorkerIdPair getJobAndWorkerId(String workerName) {
        String[] pair = null;
        if ((workerName != null) && (workerName.trim().length() > delimiter.length())) {
            pair = workerName.split(delimiter);
        }
        JobWorkerIdPair result = new JobWorkerIdPair();
        if ((pair == null) || (pair.length != 2)) {
            return setInvalid(result);
        }
        result.jobId = pair[0];
        String[] pair2 = pair[1].split("-");
        if (pair2.length > 1) {
            try {
                result.workerNumber = Integer.valueOf(pair2[1]);
                result.workerIndex = Integer.valueOf(pair2[0]);
            } catch (NumberFormatException e) {
                return setInvalid(result);
            }
        } else {
            result.workerNumber = -1;
            result.workerIndex = -1;
        }
        return result;
    }

    private static JobWorkerIdPair setInvalid(JobWorkerIdPair result) {
        result.jobId = "InvalidJob";
        result.workerIndex = -1;
        result.workerNumber = -1;
        return result;
    }
}
