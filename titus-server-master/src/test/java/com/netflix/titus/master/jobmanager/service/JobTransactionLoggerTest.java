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

package com.netflix.titus.master.jobmanager.service;

import java.util.Optional;
import java.util.UUID;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.JobStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations.Trigger;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ModelActionHolder;
import com.netflix.titus.master.jobmanager.service.common.action.TitusChangeAction;
import com.netflix.titus.master.jobmanager.service.common.action.TitusModelAction;
import com.netflix.titus.master.jobmanager.service.event.JobManagerReconcilerEvent;
import com.netflix.titus.master.jobmanager.service.event.JobModelReconcilerEvent.JobModelUpdateReconcilerEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class JobTransactionLoggerTest {

    private static final Logger logger = LoggerFactory.getLogger(JobTransactionLoggerTest.class);

    /**
     * Sole purpose of this test is visual inspection of the generated log line.
     */
    @Test
    public void testLogFormatting() throws Exception {
        Job previousJob = createJob();
        Job currentJob = previousJob.toBuilder().withStatus(JobStatus.newBuilder().withState(JobState.Finished).build()).build();

        ModelActionHolder modelActionHolder = ModelActionHolder.reference(
                TitusModelAction.newModelUpdate("testModelAction")
                        .job(previousJob)
                        .trigger(Trigger.API)
                        .summary("Job model update")
                        .jobUpdate(jobHolder -> jobHolder.setEntity(currentJob))
        );

        TitusChangeAction changeAction = TitusChangeAction.newAction("testChangeAction")
                .job(previousJob)
                .trigger(Trigger.API)
                .summary("Job update")
                .callMetadata(CallMetadata.newBuilder().withCallerId("LoggerTest").withCallReason("Testing logger transaction").build())
                .applyModelUpdate(self -> modelActionHolder);

        JobManagerReconcilerEvent jobReconcilerEvent = new JobModelUpdateReconcilerEvent(
                previousJob,
                changeAction,
                modelActionHolder,
                EntityHolder.newRoot(currentJob.getId(), currentJob),
                Optional.of(EntityHolder.newRoot(previousJob.getId(), previousJob)),
                "1"

        );
        String logLine = JobTransactionLogger.doFormat(jobReconcilerEvent);
        assertThat(logLine).isNotEmpty();

        logger.info("Job event: {}", logLine);
    }

    private Job createJob() {
        return JobModel.newJob()
                .withId(UUID.randomUUID().toString())
                .withStatus(JobModel.newJobStatus().withState(JobState.Accepted).build())
                .withJobDescriptor(JobModel.newJobDescriptor().build())
                .build();
    }
}