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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.util.retry.Retryer;
import io.netflix.titus.common.util.retry.Retryers;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.SampleTitusChangeActions;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class RetryActionInterceptorTest {

    private static final String ATTR_NAME = "test.retry";

    private static final long INITIAL_DELAY_MS = 100;
    private static final long MAX_DELAY_MS = 1000;
    private static final int RETRY_LIMIT = 3;

    private static final Retryer RETRY_POLICY = Retryers.exponentialBackoff(INITIAL_DELAY_MS, MAX_DELAY_MS, TimeUnit.MILLISECONDS, RETRY_LIMIT);

    private TestScheduler testScheduler = Schedulers.test();

    private final RetryActionInterceptor retryInterceptor = new RetryActionInterceptor(ATTR_NAME, RETRY_POLICY, testScheduler);

    private final ExtTestSubscriber<Pair<JobChange, List<ModelUpdateAction>>> testSubscriber = new ExtTestSubscriber<>();

    @Test
    public void testSuccessfulActionsPassThrough() throws Exception {
        retryInterceptor.apply(SampleTitusChangeActions.successfulJob()).apply().subscribe(testSubscriber);

        Pair<JobChange, List<ModelUpdateAction>> updateAction = testSubscriber.takeNext();
        assertThat(updateAction.getRight().get(0)).isInstanceOf(RetryActionInterceptor.RemoveRetryRecord.class);

        testSubscriber.assertOnCompleted();
    }

    @Test
    public void testRetry() throws Exception {
        TitusChangeAction changeAction = SampleTitusChangeActions.failingJob(2);

        // First two calls should fail
        ModelUpdateAction updateAction1 = expectUpdateActionOfType(changeAction, RetryActionInterceptor.RetryModelUpdateAction.class);
        EntityHolder modelWithTag1 = expectAboveExecutionLimits(updateAction1, EntityHolder.newRoot("rootId", "data"));
        expectBelowExecutionLimitsWhenTimeAdvanced(modelWithTag1, INITIAL_DELAY_MS);

        ModelUpdateAction updateAction2 = expectUpdateActionOfType(changeAction, RetryActionInterceptor.RetryModelUpdateAction.class);
        EntityHolder modelWithTag2 = expectAboveExecutionLimits(updateAction2, modelWithTag1);
        expectBelowExecutionLimitsWhenTimeAdvanced(modelWithTag2, INITIAL_DELAY_MS * 2);

        // Third call should succeed
        ModelUpdateAction updateAction3 = expectUpdateActionOfType(changeAction, RetryActionInterceptor.RemoveRetryRecord.class);
        expectNoRetryTag(updateAction3, modelWithTag2);
    }

    private ModelUpdateAction expectUpdateActionOfType(TitusChangeAction changeAction, Class<? extends ModelUpdateAction> updateActionType) {
        ExtTestSubscriber<Pair<JobChange, List<ModelUpdateAction>>> testSubscriber = new ExtTestSubscriber<>();
        retryInterceptor.apply(changeAction).apply().subscribe(testSubscriber);

        Pair<JobChange, List<ModelUpdateAction>> updateAction = testSubscriber.takeNext();
        assertThat(updateAction.getRight().get(0)).isInstanceOf(updateActionType);
        return updateAction.getRight().get(0);
    }

    private EntityHolder expectAboveExecutionLimits(ModelUpdateAction updateAction, EntityHolder model) {
        Optional<EntityHolder> modelWithTagOpt = updateAction.apply(model).getRight();
        assertThat(modelWithTagOpt).isPresent();

        EntityHolder modelWithTag = modelWithTagOpt.get();
        assertThat(retryInterceptor.executionLimits(modelWithTag)).isFalse();
        return modelWithTag;
    }

    private void expectBelowExecutionLimitsWhenTimeAdvanced(EntityHolder modelWithTag, long delayMs) {
        testScheduler.advanceTimeBy(delayMs / 2, TimeUnit.MILLISECONDS);
        assertThat(retryInterceptor.executionLimits(modelWithTag)).isFalse();

        testScheduler.advanceTimeBy(delayMs, TimeUnit.MILLISECONDS);
        assertThat(retryInterceptor.executionLimits(modelWithTag)).isTrue();
    }

    private void expectNoRetryTag(ModelUpdateAction updateAction, EntityHolder model) {
        Optional<EntityHolder> modelWithoutTag = updateAction.apply(model).getRight();
        assertThat(modelWithoutTag).isPresent();
        assertThat(modelWithoutTag.get().getAttributes()).isEmpty();
    }
}