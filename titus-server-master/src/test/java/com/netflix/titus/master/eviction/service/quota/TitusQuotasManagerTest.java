package com.netflix.titus.master.eviction.service.quota;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.master.eviction.service.quota.job.JobQuotaController;
import com.netflix.titus.master.eviction.service.quota.system.SystemQuotaConsumptionResults;
import com.netflix.titus.master.eviction.service.quota.system.SystemQuotaController;
import com.netflix.titus.runtime.connector.eviction.EvictionConfiguration;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.withApplicationName;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TitusQuotasManagerTest {
    @Test
    public void tryConsumeSystemAndJobQuota() {
        String taskId = "job1Task1";
        String jobQuotaRejectionReason = "Job does not allow any more terminations";

        Job<BatchJobExt> job1 = JobGenerator.oneBatchJob().but(withApplicationName("app1Test"));
        EvictionConfiguration config1 = mock(EvictionConfiguration.class);
        when(config1.getAppsExemptFromSystemDisruptionBudget()).thenReturn("app1.*");
        SystemQuotaController systemQuotaController = mock(SystemQuotaController.class);
        when(systemQuotaController.consume(taskId)).thenReturn(ConsumptionResult.approved());
        JobQuotaController jobQuotaController = mock(JobQuotaController.class);
        when(jobQuotaController.consume(taskId)).thenReturn(ConsumptionResult.rejected(jobQuotaRejectionReason));

        TitusQuotasManager titusQuotasManager = new TitusQuotasManager(null, null, null, systemQuotaController,
                config1, null);

        ConsumptionResult consumptionResult = titusQuotasManager.tryConsumeSystemAndJobQuota(jobQuotaController, job1, taskId);
        assertThat(consumptionResult.isApproved()).isFalse();
        assertThat(consumptionResult.getRejectionReason()).isPresent();
        assertThat(consumptionResult.getRejectionReason().get()).isEqualTo(jobQuotaRejectionReason);

        JobQuotaController jobQuotaController2 = mock(JobQuotaController.class);
        when(jobQuotaController2.consume(taskId)).thenReturn(ConsumptionResult.approved());
        ConsumptionResult consumptionResult2 = titusQuotasManager.tryConsumeSystemAndJobQuota(jobQuotaController2, job1, taskId);
        assertThat(consumptionResult2.isApproved()).isTrue();

        String quotaLimitExceededReason = SystemQuotaConsumptionResults.QUOTA_LIMIT_EXCEEDED.getRejectionReason().get();
        when(systemQuotaController.consume(taskId)).thenReturn(ConsumptionResult.rejected(quotaLimitExceededReason));
        ConsumptionResult consumptionResult3 = titusQuotasManager.tryConsumeSystemAndJobQuota(jobQuotaController2, job1, taskId);
        assertThat(consumptionResult3.isApproved()).isFalse();
        assertThat(consumptionResult3.getRejectionReason()).isPresent();
        assertThat(consumptionResult3.getRejectionReason().get()).isEqualTo(quotaLimitExceededReason);

        String outsideSystemWindowReason = SystemQuotaConsumptionResults.OUTSIDE_SYSTEM_TIME_WINDOW.getRejectionReason().get();
        when(systemQuotaController.consume(taskId)).thenReturn(ConsumptionResult.rejected(outsideSystemWindowReason));
        ConsumptionResult consumptionResult4 = titusQuotasManager.tryConsumeSystemAndJobQuota(jobQuotaController2, job1, taskId);
        assertThat(consumptionResult4.isApproved()).isTrue();
    }

    @Test
    public void isJobExemptFromSystemDisruptionBudget() {
        Job<BatchJobExt> job1 = JobGenerator.oneBatchJob().but(withApplicationName("app1Test"));

        EvictionConfiguration config1 = mock(EvictionConfiguration.class);
        when(config1.getAppsExemptFromSystemDisruptionBudget()).thenReturn("app1.*");
        TitusQuotasManager titusQuotasManager = new TitusQuotasManager(null, null, null, null,
                config1, null);
        boolean jobExemptFromSystemDisruptionBudget = titusQuotasManager.isJobExemptFromSystemDisruptionWindow(job1);
        assertThat(jobExemptFromSystemDisruptionBudget).isTrue();

        EvictionConfiguration config2 = mock(EvictionConfiguration.class);
        when(config2.getAppsExemptFromSystemDisruptionBudget()).thenReturn("app2.*");
        TitusQuotasManager titusQuotasManager2 = new TitusQuotasManager(null, null, null, null,
                config2, null);
        boolean jobExemptFromSystemDisruptionBudget2 = titusQuotasManager2.isJobExemptFromSystemDisruptionWindow(job1);
        assertThat(jobExemptFromSystemDisruptionBudget2).isFalse();
    }
}