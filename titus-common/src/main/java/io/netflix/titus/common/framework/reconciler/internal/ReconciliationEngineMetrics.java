package io.netflix.titus.common.framework.reconciler.internal;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.netflix.titus.common.framework.reconciler.ChangeAction;
import io.netflix.titus.common.util.time.Clock;

class ReconciliationEngineMetrics<EVENT> {

    private static final String ROOT_NAME = "titus.reconciliation.engine.";
    private static final String EVALUATIONS = ROOT_NAME + "evaluations";
    private static final String EVENTS_AND_MODEL_UPDATES = ROOT_NAME + "eventsAndModelUpdatesId";
    private static final String PENDING_CHANGE_ACTIONS = ROOT_NAME + "pendingChangeActions";
    private static final String STARTED_CHANGE_ACTIONS = ROOT_NAME + "startedChangeActions";
    private static final String FINISHED_CHANGE_ACTIONS = ROOT_NAME + "finishedChangeActions";
    private static final String EMITTED_EVENTS = ROOT_NAME + "emittedEvents";

    private final Function<ChangeAction, List<Tag>> extraChangeActionTags;
    private final Function<EVENT, List<Tag>> extraModelActionTags;
    private final Registry registry;
    private final Clock clock;

    private final Id evaluationId;
    private final Id eventsAndModelUpdatesId;
    private final Id startedChangeActionsId;
    private final Id finishedChangeActionId;
    private final Id emittedEventId;

    private final AtomicLong pendingChangeActions = new AtomicLong();

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
        this.eventsAndModelUpdatesId = registry.createId(EVENTS_AND_MODEL_UPDATES, commonTags);
        this.startedChangeActionsId = registry.createId(STARTED_CHANGE_ACTIONS, commonTags);
        this.finishedChangeActionId = registry.createId(FINISHED_CHANGE_ACTIONS, commonTags);
        this.emittedEventId = registry.createId(EMITTED_EVENTS, commonTags);

        PolledMeter.using(registry).withName(PENDING_CHANGE_ACTIONS).withTags(commonTags).monitorValue(pendingChangeActions);
    }

    void shutdown() {
        pendingChangeActions.set(0);
    }

    void evaluated(long executionTimeNs) {
        registry.timer(evaluationId).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void evaluated(long executionTimeNs, Exception error) {
        registry.timer(evaluationId.withTag("error", error.getClass().getSimpleName())).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    public void eventsAndModelUpdates(long executionTimeNs) {
        registry.timer(eventsAndModelUpdatesId).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void eventsAndModelUpdates(long executionTimeNs, Exception error) {
        registry.timer(eventsAndModelUpdatesId.withTag("error", error.getClass().getSimpleName())).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    void changeActionStarted(ChangeActionHolder actionHolder) {
        changeActionStarted(actionHolder, "change");
    }

    void reconcileActionStarted(ChangeActionHolder actionHolder) {
        changeActionStarted(actionHolder, "reconcile");
    }

    void changeActionFinished(ChangeActionHolder actionHolder, long executionTimeNs) {
        changeActionFinished(actionHolder, executionTimeNs, "change", false);
    }

    void reconcileActionFinished(ChangeActionHolder actionHolder, long executionTimeNs) {
        changeActionFinished(actionHolder, executionTimeNs, "reconcile", false);
    }

    void changeActionUnsubscribed(ChangeActionHolder actionHolder, long executionTimeNs) {
        changeActionFinished(actionHolder, executionTimeNs, "change", true);
    }

    void reconcileActionUnsubscribed(ChangeActionHolder actionHolder, long executionTimeNs) {
        changeActionFinished(actionHolder, executionTimeNs, "reconcile", true);
    }

    void changeActionFinished(ChangeActionHolder actionHolder, long executionTimeNs, Throwable error) {
        changeActionFinished(actionHolder, executionTimeNs, error, "change");
    }

    void reconcileActionFinished(ChangeActionHolder actionHolder, long executionTimeNs, Throwable error) {
        changeActionFinished(actionHolder, executionTimeNs, error, "reconcile");
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

    private void changeActionStarted(ChangeActionHolder actionHolder, String actionType) {
        pendingChangeActions.incrementAndGet();
        registry.timer(startedChangeActionsId
                .withTags(extraChangeActionTags.apply(actionHolder.getChangeAction()))
                .withTag("actionType", actionType)
        ).record(clock.wallTime() - actionHolder.getCreateTimeMs(), TimeUnit.MILLISECONDS);
    }

    private void changeActionFinished(ChangeActionHolder actionHolder, long executionTimeNs, String actionType, boolean isUnsubscribe) {
        pendingChangeActions.decrementAndGet();
        registry.timer(finishedChangeActionId
                .withTags(extraChangeActionTags.apply(actionHolder.getChangeAction()))
                .withTag("actionType", actionType)
                .withTag("status", isUnsubscribe ? "unsubscribed" : "success")
        ).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }

    private void changeActionFinished(ChangeActionHolder actionHolder, long executionTimeNs, Throwable error, String actionType) {
        pendingChangeActions.decrementAndGet();
        registry.timer(finishedChangeActionId
                .withTags(extraChangeActionTags.apply(actionHolder.getChangeAction()))
                .withTag("actionType", actionType)
                .withTag("error", error.getClass().getSimpleName())
                .withTag("status", "error")
        ).record(executionTimeNs, TimeUnit.NANOSECONDS);
    }
}
