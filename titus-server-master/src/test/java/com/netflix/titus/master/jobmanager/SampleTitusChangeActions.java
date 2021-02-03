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

package com.netflix.titus.master.jobmanager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import rx.Observable;

/**
 * A collection of {@link TitusChangeAction}s, and methods for testing.
 */
public final class SampleTitusChangeActions {

    private static final CallMetadata TEST_CALLMETADATA = CallMetadata.newBuilder().withCallerId("junit").build();

    public static TitusChangeAction successfulJob() {
        return new SuccessfulChangeAction(V3JobOperations.Trigger.API, "jobId");
    }

    public static TitusChangeAction failingJob(int failureCount) {
        return new FailingChangeAction(V3JobOperations.Trigger.API, "jobId", failureCount);
    }

    private static class SuccessfulChangeAction extends TitusChangeAction {

        private SuccessfulChangeAction(Trigger trigger, String id) {
            super(trigger, id, "simulatedChangeAction", "Simulated successful action", TEST_CALLMETADATA);
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            return Observable.just(Collections.emptyList());
        }
    }

    private static class FailingChangeAction extends TitusChangeAction {

        private final AtomicInteger failureCounter;

        protected FailingChangeAction(Trigger trigger, String id, int failureCount) {
            super(trigger, id, "simulatedFailingAction", "Simulated initial failure repeated " + failureCount + " times", TEST_CALLMETADATA);
            this.failureCounter = new AtomicInteger(failureCount);
        }

        @Override
        public Observable<List<ModelActionHolder>> apply() {
            if (failureCounter.decrementAndGet() >= 0) {
                return Observable.error(new RuntimeException("Simulated failure; remaining failures=" + failureCounter.get()));
            }
            return Observable.just(Collections.emptyList());
        }
    }
}
