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

package com.netflix.titus.master.eviction.service.quota;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionFactory;
import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.model.event.EvictionQuotaEvent;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.eviction.service.EvictionServiceConfiguration;
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.hourlyRatePercentage;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.newBatchJob;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.numberOfHealthyPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.officeHourTimeWindow;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuotaEventEmitterTest {

    private static final EvictionServiceConfiguration CONFIGURATION = mock(EvictionServiceConfiguration.class);

    private static final long UPDATE_INTERVAL_MS = 10L;

    private static final EvictionQuota SYSTEM_EVICTION_QUOTA = EvictionQuota.systemQuota(100);
    public static final Duration EVENT_TIMEOUT = Duration.ofSeconds(5);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final JobComponentStub jobComponentStub = new JobComponentStub(titusRuntime);

    private final TitusQuotasManager quotasManager = mock(TitusQuotasManager.class);

    private final QuotaEventEmitter quotaEventEmitter = new QuotaEventEmitter(
            CONFIGURATION,
            jobComponentStub.getJobOperations(),
            quotasManager,
            titusRuntime
    );

    @BeforeClass
    public static void setUpClass() {
        when(CONFIGURATION.getEventStreamQuotaUpdateIntervalMs()).thenReturn(UPDATE_INTERVAL_MS);
    }

    @Before
    public void setUp() throws Exception {
        when(quotasManager.getSystemEvictionQuota()).thenReturn(SYSTEM_EVICTION_QUOTA);
    }

    @After
    public void tearDown() {
        quotaEventEmitter.shutdown();
    }

    @Test
    public void testOneSubscription() throws Exception {
        TitusRxSubscriber<EvictionEvent> eventSubscriber = subscribeAndCheckSnapshot();

        // Create one job and check its quota is emitted
        Job<BatchJobExt> job = newBatchJob(
                5,
                budget(numberOfHealthyPolicy(8), hourlyRatePercentage(50), singletonList(officeHourTimeWindow()))
        );
        when(quotasManager.findJobEvictionQuota(job.getId())).thenReturn(Optional.of(EvictionQuota.jobQuota(job.getId(), 2)));

        jobComponentStub.createJob(job);
        jobComponentStub.createDesiredTasks(job);

        expectJobQuotaEvent(eventSubscriber, job, 2);

        // Now change the quota
        when(quotasManager.findJobEvictionQuota(job.getId())).thenReturn(Optional.of(EvictionQuota.jobQuota(job.getId(), 5)));
        expectJobQuotaEvent(eventSubscriber, job, 5);

        cancelSubscriptionAndCheckIfSucceeded(eventSubscriber);
    }

    @Test
    public void testManySubscriptions() throws InterruptedException {
        List<TitusRxSubscriber<EvictionEvent>> subscribers = asList(subscribeAndCheckSnapshot(), subscribeAndCheckSnapshot());
        cancelSubscriptionAndCheckIfSucceeded(subscribers.get(1));

        TitusRxSubscriber<EvictionEvent> remaining = subscribers.get(0);
        assertThat(remaining.isOpen()).isTrue();
    }

    private TitusRxSubscriber<EvictionEvent> subscribeAndCheckSnapshot() throws InterruptedException {
        TitusRxSubscriber<EvictionEvent> eventSubscriber = new TitusRxSubscriber<>();
        quotaEventEmitter.events(true).subscribe(eventSubscriber);

        // Check snapshot
        expectSystemQuota(eventSubscriber);
        expectSnapshotEnd(eventSubscriber);
        return eventSubscriber;
    }

    private void expectJobQuotaEvent(TitusRxSubscriber<EvictionEvent> eventSubscriber, Job job, int quota) throws InterruptedException {
        EvictionQuotaEvent event = (EvictionQuotaEvent) eventSubscriber.takeNext(EVENT_TIMEOUT);
        assertThat(event.getQuota().getReference()).isEqualTo(Reference.job(job.getId()));
        assertThat(event.getQuota().getQuota()).isEqualTo(quota);
    }

    private void expectSnapshotEnd(TitusRxSubscriber<EvictionEvent> eventSubscriber) throws InterruptedException {
        assertThat(eventSubscriber.takeNext(EVENT_TIMEOUT)).isEqualTo(EvictionEvent.newSnapshotEndEvent());
    }

    private void expectSystemQuota(TitusRxSubscriber<EvictionEvent> eventSubscriber) throws InterruptedException {
        EvictionQuotaEvent event = (EvictionQuotaEvent) eventSubscriber.takeNext(EVENT_TIMEOUT);
        assertThat(event.getQuota().getReference()).isEqualTo(Reference.global());
    }

    private void cancelSubscriptionAndCheckIfSucceeded(TitusRxSubscriber<EvictionEvent> eventSubscriber) {
        int current = quotaEventEmitter.eventSubscriberSinks.size();
        eventSubscriber.dispose();
        await().until(() -> quotaEventEmitter.eventSubscriberSinks.size() == current - 1);
    }

    private ConditionFactory await() {
        return Awaitility.await().timeout(EVENT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}