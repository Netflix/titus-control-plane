package com.netflix.titus.master.jobmanager.service.limiter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import org.junit.Test;

import static com.netflix.titus.testkit.model.job.JobGenerator.serviceJobs;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultJobSubmitLimiterTest {

    @Test
    public void checkDuplicateJobSequence() {
        JobManagerConfiguration config = mock(JobManagerConfiguration.class);
        when(config.getMaxActiveJobs()).thenReturn(2L);
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        DefaultJobSubmitLimiter limiter = new DefaultJobSubmitLimiter(config, v3JobOperations);

        Job<ServiceJobExt> job = serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        List<Job> currentJobs = new ArrayList<>();
        currentJobs.add(job);
        when(v3JobOperations.getJobs()).thenReturn(currentJobs);

        // Using the same job description containing the existing job group (stack, detail, sequence)
        Optional<JobManagerException> result = limiter.checkIfAllowed(job.getJobDescriptor());
        assertThat(result).isNotNull();
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isNotNull();
        assertThat(result.get().getErrorCode()).isEqualTo(JobManagerException.ErrorCode.InvalidSequenceId);
    }

    @Test
    public void checkMaxActiveJobsLimit() {
        JobManagerConfiguration config = mock(JobManagerConfiguration.class);
        when(config.getMaxActiveJobs()).thenReturn(1L);
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);
        DefaultJobSubmitLimiter limiter = new DefaultJobSubmitLimiter(config, v3JobOperations);

        Job<ServiceJobExt> job = serviceJobs(JobDescriptorGenerator.oneTaskServiceJobDescriptor()).getValue();
        List<Job> currentJobs = new ArrayList<>();
        currentJobs.add(job);
        when(v3JobOperations.getJobs()).thenReturn(currentJobs);

        // Max active jobs already running
        Optional<JobManagerException> result = limiter.checkIfAllowed(job.getJobDescriptor());
        assertThat(result).isNotNull();
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isNotNull();
        assertThat(result.get().getErrorCode()).isEqualTo(JobManagerException.ErrorCode.JobCreateLimited);
    }
}