package com.netflix.titus.supplementary.relocation.descheduler;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.service.ReadOnlyEvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;

class EvictionQuotaTracker {

    private final Map<String, Long> jobEvictionQuotas = new HashMap<>();
    private long systemEvictionQuota;

    EvictionQuotaTracker(ReadOnlyEvictionOperations evictionOperations, Map<String, Job<?>> jobs) {
        this.systemEvictionQuota = evictionOperations.getGlobalEvictionQuota().getQuota();
        jobs.forEach((id, job) ->
                jobEvictionQuotas.put(id, evictionOperations.findJobEvictionQuota(id).map(EvictionQuota::getQuota).orElse(0L))
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
}
