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

package io.netflix.titus.master.jobmanager.service.common.event;

import java.util.Optional;
import java.util.UUID;

import io.netflix.titus.api.jobmanager.model.event.JobEvent;
import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent.Trigger;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.api.jobmanager.model.job.JobState;
import io.netflix.titus.common.framework.reconciler.ModelActionHolder;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent.EventType;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class JobEventTest {

    private static final Logger logger = LoggerFactory.getLogger(JobEventTest.class);

    /**
     * Sole purpose of this test is visual inspection of the generated log line.
     */
    @Test
    public void testLogFormatting() throws Exception {
        Job job = createJob();
        JobEvent jobEvent = new JobEvent(
                EventType.Changed,
                ModelActionHolder.reference(TitusModelUpdateActions.updateJob(job, Trigger.API, "test")),
                Optional.empty()
        );
        String logLine = jobEvent.toLogString();
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