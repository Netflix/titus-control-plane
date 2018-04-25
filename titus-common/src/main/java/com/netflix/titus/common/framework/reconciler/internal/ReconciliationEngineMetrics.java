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

package com.netflix.titus.common.framework.reconciler.internal;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.util.time.Clock;

class ReconciliationEngineMetrics<EVENT> {

    private static final String ROOT_NAME = "titus.reconciliation.engine.";
    private static final String EVALUATIONS = ROOT_NAME + "evaluations";
    private static final String PENDING_CHANGE_ACTIONS = ROOT_NAME + "pendingChangeActions";
    private static final String STARTED_CHANGE_ACTIONS = ROOT_NAME + "startedChangeActions";
    private static final String FINISHED_CHANGE_ACTIONS = ROOT_NAME + "finishedChangeActions";
    private static final String EMITTED_EVENTS = ROOT_NAME + "emittedEvents";

    private final Function<ChangeAction, List<Tag>> extraChangeActionTags;
    private final Function<EVENT, List<Tag>> extraModelActionTags;
    private final Registry registry;
    private final Clock clock;

    private final Id evaluationId;
    private final Id startedChangeActionsId;
    private final Id finishedChangeActionId;
    private final Id emittedEventId;

    private final AtomicLong pendingChangeActions = new AtomicLong();
    private final Gauge changeActionQueueSize;

    ReconciliationEngineMetrics(String rootHolderId,
                                Function<ChangeAction, List<Tag>> extraChangeActionTags,
                                Function<EVENT, List<Tag>> extraModelActionTags,
                                Registry registry,
                                Clock clock) {
        this.extraChangeActionTags = extraChangeActionTags;
        this.extraModelActionTags = extraModelActionTags;
        this.registry = registry;
        this.clock = clock;

        List<Tag> commonTags = Collections.singletonList(new BasicTag("rootHolderId", rootHolderId));
        this.evaluationId = registry.createId(EVALUATIONS, commonTags);
        this.startedChangeActionsId = registry.createId(STARTED_CHANGE_ACTIONS, commonTags);
        this.finishedChangeActionId = registry.createId(FINISHED_CHANGE_ACTIONS, commonTags);
        this.emittedEventId = registry.createId(EMITTED_EVENTS, commonTags);

        this.changeActionQueueSize = registry.gauge(registry.createId(ROOT_NAME + "changeActionQueueSize", commonTags));
        PolledMeter.using(registry).withName(PENDING_CHANGE_ACTIONS).withTags(commonTags).monitorValue(pendingChangeActions);
    }

    void shutdown() {
        changeActionQueueSize.set(0);
        pendingChangeActions.set(0);
    }

    void evaluated(long executionTimeNs) {
        registry.timer(evaluationId).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void evaluated(long executionTimeNs, Exception error) {
        registry.timer(evaluationId.withTag("error", error.getClass().getSimpleName())).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void updateChangeActionQueueSize(int queueSize) {
        changeActionQueueSize.set(queueSize);
    }

    void changeActionStarted(ChangeAction actionHolder, long createTimeMs, boolean byReconciler) {
        pendingChangeActions.incrementAndGet();
        registry.timer(startedChangeActionsId
                .withTags(extraChangeActionTags.apply(actionHolder))
                .withTag("actionType", toActionType(byReconciler))
        ).record(clock.wallTime() - createTimeMs, TimeUnit.MILLISECONDS);
    }

    void changeActionFinished(ChangeAction actionHolder, long executionTimeNs, boolean byReconciler) {
        changeActionFinished(actionHolder, executionTimeNs, false, byReconciler);
    }

    void changeActionUnsubscribed(ChangeAction actionHolder, long executionTimeNs, boolean byReconciler) {
        changeActionFinished(actionHolder, executionTimeNs, true, byReconciler);
    }

    void changeActionFinished(ChangeAction actionHolder, long executionTimeNs, Throwable error, boolean byReconciler) {
        pendingChangeActions.decrementAndGet();
        registry.timer(finishedChangeActionId
                .withTags(extraChangeActionTags.apply(actionHolder))
                .withTag("actionType", toActionType(byReconciler))
                .withTag("error", error.getClass().getSimpleName())
                .withTag("status", "error")
        ).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void emittedEvent(EVENT event, long latencyNs) {
        registry.timer(emittedEventId
                .withTags(extraModelActionTags.apply(event))
                .withTag("status", "success")
        ).record(latencyNs, TimeUnit.NANOSECONDS);
    }

    void emittedEvent(EVENT event, long latencyNs, Exception error) {
        registry.timer(emittedEventId
                .withTags(extraModelActionTags.apply(event))
                .withTag("error", error.getClass().getSimpleName())
                .withTag("status", "error")
        ).record(latencyNs, TimeUnit.NANOSECONDS);
    }

    private void changeActionFinished(ChangeAction actionHolder, long executionTimeNs, boolean isUnsubscribe, boolean byReconciler) {
        pendingChangeActions.decrementAndGet();
        registry.timer(finishedChangeActionId
                .withTags(extraChangeActionTags.apply(actionHolder))
                .withTag("actionType", toActionType(byReconciler))
                .withTag("status", isUnsubscribe ? "unsubscribed" : "success")
        ).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    private String toActionType(boolean byReconciler) {
        return byReconciler ? "reconcile" : "change";
    }
}
