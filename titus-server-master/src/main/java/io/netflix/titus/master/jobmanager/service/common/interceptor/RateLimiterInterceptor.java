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

import java.util.Collections;
import java.util.List;

import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;

/**
 * {@link ChangeAction} interceptor that tracks and limits execution rate of an action.Rate limiting is controlled by
 * the provided token bucket.
 */
public class RateLimiterInterceptor implements TitusChangeActionInterceptor<Long> {

    private static final String ATTR_RATE_LIMITER_PREFIX = "interceptor.rateLimiter.";

    private final String attrName;
    private final ImmutableTokenBucket initialTokenBucket;

    public RateLimiterInterceptor(String name, ImmutableTokenBucket tokenBucket) {
        this.attrName = ATTR_RATE_LIMITER_PREFIX + name;
        this.initialTokenBucket = tokenBucket;
    }

    @Override
    public TitusChangeAction apply(TitusChangeAction delegate) {
        return TitusChangeAction.newInterceptor("rateLimited", delegate)
                .changeWithModelUpdates(self ->
                        delegate.apply().map(result -> handleSuccess(delegate, result)).onErrorReturn(e -> handleError(delegate, e))
                );
    }

    private List<ModelActionHolder> handleSuccess(TitusChangeAction delegate, List<ModelActionHolder> result) {
        return CollectionsExt.copyAndAdd(result, createRateLimiterModelAction(delegate));
    }

    private List<ModelActionHolder> handleError(TitusChangeAction delegate, Throwable error) {
        return Collections.singletonList(createRateLimiterModelAction(delegate));
    }

    private ModelActionHolder createRateLimiterModelAction(TitusChangeAction delegate) {
        TitusModelAction modelAction = TitusModelAction.newModelUpdate("rateLimiter(" + delegate.getName() + ')', delegate)
                .summary("Acquire rate limiter token from its tokenBucket")
                .jobMaybeUpdate(rootHolder -> {
                    ImmutableTokenBucket lastTokenBucket = (ImmutableTokenBucket) rootHolder.getAttributes().getOrDefault(attrName, initialTokenBucket);
                    return lastTokenBucket.tryTake().map(newBucket -> rootHolder.addTag(attrName, newBucket));
                });
        return ModelActionHolder.reference(modelAction);
    }

    @Override
    public Long executionLimits(EntityHolder rootHolder) {
        ImmutableTokenBucket lastTokenBucket = (ImmutableTokenBucket) rootHolder.getAttributes().getOrDefault(attrName, initialTokenBucket);
        return lastTokenBucket.tryTake(0, Long.MAX_VALUE).map(Pair::getLeft).orElse(0L);
    }
}
