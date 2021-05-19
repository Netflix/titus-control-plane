/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler;

import java.util.List;
import java.util.function.Function;

import reactor.core.publisher.Mono;

public class ReconcilerActionProvider<DATA> {

    private final ReconcilerActionProviderPolicy policy;
    private final boolean external;
    private final Function<DATA, List<Mono<Function<DATA, DATA>>>> actionProvider;

    public ReconcilerActionProvider(ReconcilerActionProviderPolicy policy,
                                    boolean external,
                                    Function<DATA, List<Mono<Function<DATA, DATA>>>> actionProvider) {

        this.policy = policy;
        this.external = external;
        this.actionProvider = actionProvider;
    }

    public boolean isExternal() {
        return external;
    }

    public ReconcilerActionProviderPolicy getPolicy() {
        return policy;
    }

    public Function<DATA, List<Mono<Function<DATA, DATA>>>> getActionProvider() {
        return actionProvider;
    }

    public Builder<DATA> toBuilder() {
        return ReconcilerActionProvider.<DATA>newBuilder().withPolicy(policy).withExternal(external).withActionProvider(actionProvider);
    }

    public static <DATA> Builder<DATA> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<DATA> {

        private ReconcilerActionProviderPolicy policy;
        private boolean external;
        private Function<DATA, List<Mono<Function<DATA, DATA>>>> actionProvider;

        private Builder() {
        }

        public Builder<DATA> withPolicy(ReconcilerActionProviderPolicy policy) {
            this.policy = policy;
            return this;
        }

        public Builder<DATA> withExternal(boolean external) {
            this.external = external;
            return this;
        }

        public Builder<DATA> withActionProvider(Function<DATA, List<Mono<Function<DATA, DATA>>>> actionProvider) {
            this.actionProvider = actionProvider;
            return this;
        }

        public ReconcilerActionProvider<DATA> build() {
            return new ReconcilerActionProvider<>(policy, external, actionProvider);
        }
    }
}
