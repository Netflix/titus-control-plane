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
import java.util.Optional;

import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.JobChange;
import io.netflix.titus.master.jobmanager.service.common.action.JobChanges;
import io.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import rx.Observable;

/**
 * {@link ChangeAction} interceptor that tracks and limits execution rate of an action.Rate limiting is controlled by
 * the provided token bucket.
 */
public class RateLimiterInterceptor implements TitusChangeActionInterceptor<Long> {

    private static final String ATTR_RATE_LIMITER_PREFIX = "interceptor.rateLimiter.";

    private final String name;
    private final String attrName;
    private final ImmutableTokenBucket initialTokenBucket;

    public RateLimiterInterceptor(String name, ImmutableTokenBucket tokenBucket) {
        this.name = name;
        this.attrName = ATTR_RATE_LIMITER_PREFIX + name;
        this.initialTokenBucket = tokenBucket;
    }

    @Override
    public TitusChangeAction apply(TitusChangeAction titusChangeAction) {
        return new RateLimiterChangeAction(titusChangeAction);
    }

    @Override
    public Long executionLimits(EntityHolder rootHolder) {
        ImmutableTokenBucket lastTokenBucket = (ImmutableTokenBucket) rootHolder.getAttributes().getOrDefault(attrName, initialTokenBucket);
        return lastTokenBucket.tryTake(0, Long.MAX_VALUE).map(Pair::getLeft).orElse(0L);
    }

    class RateLimiterChangeAction extends TitusChangeAction {

        private final TitusChangeAction delegate;

        RateLimiterChangeAction(TitusChangeAction delegate) {
            super(new JobChange(delegate.getChange().getTrigger(), delegate.getChange().getId(), "RateLimiter(" + delegate.getChange().getName() + ')', delegate.getChange().getSummary()));
            this.delegate = delegate;
        }

        @Override
        public Observable<Pair<JobChange, List<ModelActionHolder>>> apply() {
            ModelActionHolder action = ModelActionHolder.store(new UpdateRateLimiterStateAction(delegate));
            return delegate.apply().map(
                    result -> JobChanges.wrapper(result, "RateLimiter", action)
            ).onErrorReturn(e -> Pair.of(getChange(), Collections.singletonList(action)));
        }
    }

    class UpdateRateLimiterStateAction extends TitusModelAction {
        UpdateRateLimiterStateAction(TitusChangeAction delegate) {
            super(
                    delegate.getChange().getTrigger(),
                    delegate.getChange().getId(),
                    "Updating rate limiting data of " + name
            );
        }

        @Override
        public Optional<Pair<EntityHolder, EntityHolder>> apply(EntityHolder rootHolder) {
            ImmutableTokenBucket lastTokenBucket = (ImmutableTokenBucket) rootHolder.getAttributes().getOrDefault(attrName, initialTokenBucket);
            return lastTokenBucket.tryTake()
                    .map(newBucket -> {
                        EntityHolder newRoot = rootHolder.addTag(attrName, newBucket);
                        return Pair.of(newRoot, newRoot);
                    });
        }
    }
}
