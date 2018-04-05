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

package com.netflix.titus.master.jobmanager.service.common.interceptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelAction;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.util.limiter.ImmutableLimiters;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.master.jobmanager.SampleTitusChangeActions;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.testkit.model.job.JobDescriptorGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;

import static com.netflix.titus.testkit.model.job.JobGenerator.batchJobs;
import static org.assertj.core.api.Assertions.assertThat;

public class RateLimiterInterceptorTest {

    private static final String ATTR_NAME = "test.rateLimiter";

    private static final long BUCKET_SIZE = 5;
    private static final long REFILL_INTERVAL_MS = 100;

    private final TestClock testClock = Clocks.test();

    private final ImmutableTokenBucket.ImmutableRefillStrategy refillStrategy = ImmutableLimiters.refillAtFixedInterval(
            1, REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS, testClock
    );

    private final ImmutableTokenBucket tokenBucket = ImmutableLimiters.tokenBucket(BUCKET_SIZE, refillStrategy);

    private final RateLimiterInterceptor rateLimiterInterceptor = new RateLimiterInterceptor(ATTR_NAME, tokenBucket);

    @Test
    public void testRateLimiting() throws Exception {
        Job<BatchJobExt> job = batchJobs(JobDescriptorGenerator.oneTaskBatchJobDescriptor()).getValue();

        // Use all tokens
        EntityHolder nextRoot = EntityHolder.newRoot("root", job);
        for (int i = 0; i < BUCKET_SIZE; i++) {
            assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(BUCKET_SIZE - i);
            ModelAction updateAction = executeRateLimitedAction(SampleTitusChangeActions.successfulJob());
            nextRoot = updateAction.apply(nextRoot).get().getRight();
        }
        assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(0);

        // Refill
        testClock.advanceTime(REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(1);
    }

    private ModelAction executeRateLimitedAction(TitusChangeAction changeAction) {
        ExtTestSubscriber<List<ModelActionHolder>> testSubscriber = new ExtTestSubscriber<>();
        rateLimiterInterceptor.apply(changeAction).apply().subscribe(testSubscriber);

        ModelAction updateAction = testSubscriber.takeNext().get(0).getAction();
        assertThat(updateAction).isInstanceOf(TitusModelAction.class);
        return updateAction;
    }
}