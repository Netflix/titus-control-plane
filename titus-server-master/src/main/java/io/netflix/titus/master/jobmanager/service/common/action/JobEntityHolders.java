package io.netflix.titus.master.jobmanager.service.common.action;

import java.util.Objects;
import java.util.Optional;

import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import rx.Observable;

import static io.netflix.titus.common.util.code.CodeInvariants.codeInvariants;

/**
 * Functions for processing job manager {@link EntityHolder} instances.
 */
public final class JobEntityHolders {

    public static Optional<Task> expectTask(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine, String taskId) {
        Optional<EntityHolder> taskHolder = engine.getReferenceView().findById(taskId);
        if (taskHolder.isPresent()) {
            return Optional.of(taskHolder.get().getEntity());
        }
        codeInvariants().inconsistent("Expected to find task %s owned by job %s", taskId, engine.getReferenceView().getId());
        return Optional.empty();
    }

    public static Observable<Task> toTaskObservable(ReconciliationEngine<JobChange, JobManagerReconcilerEvent> engine, String taskId) {
        return Observable.fromCallable(() -> expectTask(engine, taskId).orElse(null)).filter(Objects::nonNull);
    }

    public static Pair<EntityHolder, EntityHolder> addTask(EntityHolder rootHolder, Task newTask) {
        EntityHolder newTaskHolder = rootHolder.findById(newTask.getId())
                .map(taskHolder -> taskHolder.setEntity(newTask))
                .orElseGet(() -> EntityHolder.newRoot(newTask.getId(), newTask));
        return Pair.of(rootHolder.addChild(newTaskHolder), newTaskHolder);
    }
}
