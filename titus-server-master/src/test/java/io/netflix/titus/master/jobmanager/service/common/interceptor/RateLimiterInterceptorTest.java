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

package io.netflix.titus.master.jobmanager.service.common.interceptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.util.limiter.ImmutableLimiters;
import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.time.TestClock;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.SampleTitusChangeActions;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;

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
        // Use all tokens
        EntityHolder nextRoot = EntityHolder.newRoot("rootId", "data");
        for (int i = 0; i < BUCKET_SIZE; i++) {
            assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(BUCKET_SIZE - i);
            ModelUpdateAction updateAction = expectUpdateActionOfType(SampleTitusChangeActions.successfulJob(), RateLimiterInterceptor.UpdateRateLimiterStateAction.class);
            nextRoot = updateAction.apply(nextRoot).getRight().get();
        }
        assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(0);

        // Refill
        testClock.advanceTime(REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(rateLimiterInterceptor.executionLimits(nextRoot)).isEqualTo(1);
    }

    private ModelUpdateAction expectUpdateActionOfType(TitusChangeAction changeAction, Class<? extends ModelUpdateAction> updateActionType) {
        ExtTestSubscriber<Pair<JobChange, List<ModelUpdateAction>>> testSubscriber = new ExtTestSubscriber<>();
        rateLimiterInterceptor.apply(changeAction).apply().subscribe(testSubscriber);

        ModelUpdateAction updateAction = testSubscriber.takeNext().getRight().get(0);
        assertThat(updateAction).isInstanceOf(updateActionType);
        return updateAction;
    }
}