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

package com.netflix.titus.testkit.cli.command.job;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.common.util.event.EventPropagationTrace;
import com.netflix.titus.runtime.connector.jobmanager.JobEventPropagationMetrics;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.cli.command.ErrorReports;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 */
public class ObserveJobsCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(ObserveJobsCommand.class);

    @Override
    public String getDescription() {
        return "observe state changes of active job(s)";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("i").longOpt("job_id").hasArg().desc("Job id").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        JobManagementClient service = context.getJobManagementClient();
        Flux<JobManagerEvent<?>> events;

        if (context.getCLI().hasOption('i')) {
            String jobId = context.getCLI().getOptionValue('i');
            events = service.observeJob(jobId);
        } else {
            events = service.observeJobs(Collections.emptyMap());
        }

        JobEventPropagationMetrics metrics = JobEventPropagationMetrics.newExternalClientMetrics("cli", context.getTitusRuntime());

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean snapshotRead = new AtomicBoolean();
        events.subscribe(
                next -> {
                    logger.info("Emitted: {}", next);
                    if (next == JobManagerEvent.snapshotMarker()) {
                        snapshotRead.set(true);
                    } else if (next instanceof JobUpdateEvent) {
                        Optional<EventPropagationTrace> trace = metrics.recordJob(((JobUpdateEvent) next).getCurrent(), !snapshotRead.get());
                        trace.ifPresent(t -> {
                            logger.info("Event propagation data: stages={}", t);
                        });
                    } else if (next instanceof TaskUpdateEvent) {
                        Optional<EventPropagationTrace> trace = metrics.recordTask(((TaskUpdateEvent) next).getCurrent(), !snapshotRead.get());
                        trace.ifPresent(t -> logger.info("Event propagation data: {}", t));
                    }
                },
                e -> {
                    ErrorReports.handleReplyError("Error in the event stream", e);
                    latch.countDown();
                },
                () -> {
                    logger.info("Event stream closed");
                    latch.countDown();
                }
        );
        latch.await();
    }
}
