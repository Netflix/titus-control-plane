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

package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.model.reference.Reference;

class EvictionQuotaTracker {

    private final Map<String, Long> jobEvictionQuotas = new HashMap<>();
    private long systemEvictionQuota;

    EvictionQuotaTracker(ReadOnlyEvictionOperations evictionOperations, Map<String, Job<?>> jobs) {
        this.systemEvictionQuota = evictionOperations.getEvictionQuota(Reference.system()).getQuota();
        jobs.forEach((id, job) ->
                jobEvictionQuotas.put(id, evictionOperations.findEvictionQuota(Reference.job(id)).map(EvictionQuota::getQuota).orElse(0L))
        );
    }

    long getSystemEvictionQuota() {
        return systemEvictionQuota;
    }

    long getJobEvictionQuota(String jobId) {
        return jobEvictionQuotas.getOrDefault(jobId, 0L);
    }

    void consumeQuota(String jobId) {
        if (systemEvictionQuota <= 0) {
            throw DeschedulerException.noQuotaLeft("System quota is empty");
        }
        if (!jobEvictionQuotas.containsKey(jobId)) {
            throw DeschedulerException.noQuotaLeft("Attempt to use quota for unknown job: jobId=%s", jobId);
        }
        long jobQuota = jobEvictionQuotas.get(jobId);
        if (jobQuota <= 0) {
            throw DeschedulerException.noQuotaLeft("Job quota is empty: jobId=%s", jobId);
        }
        systemEvictionQuota = systemEvictionQuota - 1;
        jobEvictionQuotas.put(jobId, jobQuota - 1);
    }

    /**
     * An alternative version to {@link #consumeQuota(String)} which does not throw an exception if there is not
     * enough quota to relocate a task of a given job. This is used when the immediate relocation is required.
     */
    void consumeQuotaNoError(String jobId) {
        if (systemEvictionQuota > 0) {
            systemEvictionQuota = systemEvictionQuota - 1;
        }
        long jobQuota = jobEvictionQuotas.getOrDefault(jobId, 0L);
        if (jobQuota > 0) {
            jobEvictionQuotas.put(jobId, jobQuota - 1);
        }
    }
}
