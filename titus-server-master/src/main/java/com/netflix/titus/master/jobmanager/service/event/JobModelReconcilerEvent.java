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

package com.netflix.titus.master.jobmanager.service.event;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;

import static com.netflix.titus.master.jobmanager.service.common.action.task.BasicJobActions.ATTR_JOB_CLOSED;

public abstract class JobModelReconcilerEvent extends JobManagerReconcilerEvent {

    protected JobModelReconcilerEvent(Job<?> job, String transactionId, CallMetadata callMetadata) {
        super(job, transactionId, callMetadata);
    }

    public static class JobNewModelReconcilerEvent extends JobModelReconcilerEvent {
        private final EntityHolder newRoot;

        public JobNewModelReconcilerEvent(EntityHolder newRoot) {
            super(newRoot.getEntity(), "-1", (CallMetadata) newRoot.getAttributes().get(JobManagerConstants.JOB_MANAGER_ATTRIBUTE_CALLMETADATA));
            this.newRoot = newRoot;
        }

        public EntityHolder getNewRoot() {
            return newRoot;
        }
    }

    public static class JobModelUpdateReconcilerEvent extends JobModelReconcilerEvent {

        private final TitusChangeAction changeAction;
        private final ModelActionHolder modelActionHolder;
        private final EntityHolder changedEntityHolder;
        private final Optional<EntityHolder> previousEntityHolder;
        private final boolean archived;

        public JobModelUpdateReconcilerEvent(Job<?> job,
                                             TitusChangeAction changeAction,
                                             ModelActionHolder modelActionHolder,
                                             EntityHolder changedEntityHolder,
                                             Optional<EntityHolder> previousEntityHolder,
                                             String transactionId) {
            super(job, transactionId, changeAction.getCallMetadata());
            this.changeAction = changeAction;
            this.modelActionHolder = modelActionHolder;
            this.changedEntityHolder = changedEntityHolder;
            this.previousEntityHolder = previousEntityHolder;

            if (changedEntityHolder.getEntity() instanceof Job) {
                Boolean closed = (Boolean) changedEntityHolder.getAttributes().get(ATTR_JOB_CLOSED);
                this.archived = closed != null && closed;
            } else {
                this.archived = false;
            }
        }

        public TitusChangeAction getChangeAction() {
            return changeAction;
        }

        public ModelActionHolder getModelActionHolder() {
            return modelActionHolder;
        }

        public EntityHolder getChangedEntityHolder() {
            return changedEntityHolder;
        }

        public Optional<EntityHolder> getPreviousEntityHolder() {
            return previousEntityHolder;
        }

        public boolean isArchived() {
            return archived;
        }
    }

    public static class JobModelUpdateErrorReconcilerEvent extends JobModelReconcilerEvent {

        private final TitusChangeAction changeAction;
        private final ModelActionHolder modelActionHolder;
        private final EntityHolder previousEntityHolder;
        private final Throwable error;

        public JobModelUpdateErrorReconcilerEvent(Job<?> job,
                                                  TitusChangeAction changeAction,
                                                  ModelActionHolder modelActionHolder,
                                                  EntityHolder previousEntityHolder,
                                                  Throwable error,
                                                  String transactionId,
                                                  CallMetadata callMetadata) {
            super(job, transactionId, callMetadata);
            this.changeAction = changeAction;
            this.modelActionHolder = modelActionHolder;
            this.previousEntityHolder = previousEntityHolder;
            this.error = error;
        }

        public TitusChangeAction getChangeAction() {
            return changeAction;
        }

        public ModelActionHolder getModelActionHolder() {
            return modelActionHolder;
        }

        public EntityHolder getPreviousEntityHolder() {
            return previousEntityHolder;
        }

        public Throwable getError() {
            return error;
        }
    }
}
