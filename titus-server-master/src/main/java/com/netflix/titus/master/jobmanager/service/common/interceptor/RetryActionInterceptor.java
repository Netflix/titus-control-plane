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

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine.DifferenceResolver;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;
import rx.Scheduler;

/**
 * {@link ChangeAction} interceptor that intercepts errors, and records them in a model. It also provides
 * {@link RetryActionInterceptor#executionLimits(EntityHolder)} method that can be used in {@link DifferenceResolver}
 * evaluator to check if an action should be created, or the creation should be postponed.
 * <p>
 * TODO Current implementation operates on store model.
 */
public class RetryActionInterceptor implements TitusChangeActionInterceptor<Boolean> {

    private static final String ATTR_RETRY_RECORD_PREFIX = "interceptor.retry.record.";

    private final String attrName;
    private final Retryer initialRetryPolicy;
    private final Scheduler scheduler;

    public RetryActionInterceptor(String name,
                                  Retryer initialRetryPolicy,
                                  Scheduler scheduler) {
        this.attrName = ATTR_RETRY_RECORD_PREFIX + name;
        this.initialRetryPolicy = initialRetryPolicy;
        this.scheduler = scheduler;
    }

    @Override
    public TitusChangeAction apply(TitusChangeAction changeAction) {
        return new RetryChangeAction(changeAction);
    }

    @Override
    public Boolean executionLimits(EntityHolder rootHolder) {
        RetryRecord retryRecord = (RetryRecord) rootHolder.getAttributes().get(attrName);
        if (retryRecord == null) {
            return true;
        }
        long delayMs = retryRecord.getRetryPolicy().getDelayMs().orElse(-1L);
        if (delayMs < 0) {
            return true;
        }
        long pauseTime = retryRecord.getLastFailureTime() + delayMs;
        return scheduler.now() >= pauseTime;
    }

    class RetryChangeAction extends TitusChangeAction {

        private final TitusChangeAction delegate;

        RetryChangeAction(TitusChangeAction delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            return delegate.apply().map(modelActionHolders -> CollectionsExt.copyAndAdd(modelActionHolders, ModelActionHolder.store(new RemoveRetryRecord(delegate)))
            ).onErrorReturn(e -> Collections.singletonList(ModelActionHolder.store(new RetryModelUpdateAction(delegate, e))));
        }
    }

    class RetryModelUpdateAction extends TitusModelAction {

        RetryModelUpdateAction(TitusChangeAction delegate, Throwable e) {
            super(
                    delegate.getTrigger(),
                    delegate.getId(),
                    "Report failure of: " + delegate.getSummary() + '(' + e.getMessage() + ')'
            );
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            RetryRecord retryRecord = (RetryRecord) rootHolder.getAttributes().get(attrName);

            RetryRecord newRecord;
            long now = scheduler.now();
            if (retryRecord == null) {
                newRecord = new RetryRecord(initialRetryPolicy, now, 1);
            } else if (!retryRecord.getRetryPolicy().getDelayMs().isPresent()) { // Retry limit reached
                return Optional.empty();
            } else {
                // Only increment retry for actions that happened after the last failure time
                long nextRetryTime = retryRecord.getLastFailureTime() + retryRecord.getRetryPolicy().getDelayMs().get();
                int failureCount = nextRetryTime >= now ? retryRecord.getFailureCount() : retryRecord.getFailureCount() + 1;

                newRecord = new RetryRecord(retryRecord.getRetryPolicy().retry(), now, failureCount);
            }

            EntityHolder newRoot = rootHolder.addTag(attrName, newRecord);
            return Optional.of(Pair.of(newRoot, newRoot));
        }
    }

    class RemoveRetryRecord extends TitusModelAction {
        RemoveRetryRecord(TitusChangeAction delegate) {
            super(
                    delegate.getTrigger(),
                    delegate.getId(),
                    "Cleaning up after successful action execution"
            );
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            if (rootHolder.getAttributes().containsKey(attrName)) {
                EntityHolder newRoot = rootHolder.removeTag(attrName);
                return Optional.of(Pair.of(newRoot, newRoot));
            }
            return Optional.empty();
        }
    }

    static class RetryRecord {
        private final Retryer retryPolicy;
        private final long lastFailureTime;
        private final int failureCount;

        RetryRecord(Retryer retryPolicy, long lastFailureTime, int failureCount) {
            this.retryPolicy = retryPolicy;
            this.lastFailureTime = lastFailureTime;
            this.failureCount = failureCount;
        }

        Retryer getRetryPolicy() {
            return retryPolicy;
        }

        long getLastFailureTime() {
            return lastFailureTime;
        }

        int getFailureCount() {
            return failureCount;
        }
    }
}
