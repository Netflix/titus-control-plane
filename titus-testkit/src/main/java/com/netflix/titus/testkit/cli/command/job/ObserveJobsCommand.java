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

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.JobKeepAliveEvent;
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
 *
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
        options.addOption(Option.builder("l").longOpt("latency").desc("If set, print the propagation latency").build());
        options.addOption(Option.builder("k").longOpt("keepalive").hasArg().desc("If set, use the keep alive enabled client with the configured interval").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        long keepAliveMs = context.getCLI().hasOption('k') ? Long.parseLong(context.getCLI().getOptionValue('k')) : -1;
        JobManagementClient service = keepAliveMs > 0 ? context.getJobManagementClientWithKeepAlive(keepAliveMs) : context.getJobManagementClient();
        Flux<JobManagerEvent<?>> events;

        boolean printLatency = context.getCLI().hasOption('l');

        if (context.getCLI().hasOption('i')) {
            String jobId = context.getCLI().getOptionValue('i');
            events = service.observeJob(jobId);
        } else {
            events = service.observeJobs(Collections.emptyMap());
        }

        JobEventPropagationMetrics metrics = JobEventPropagationMetrics.newExternalClientMetrics("cli", context.getTitusRuntime());

        while (true) {
            logger.info("Establishing a new connection to the job event stream endpoint...");
            executeOnce(events, metrics, printLatency);
        }
    }

    private void executeOnce(Flux<JobManagerEvent<?>> events, JobEventPropagationMetrics metrics, boolean printLatency) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean snapshotRead = new AtomicBoolean();
        events.subscribe(
                next -> {
                    if (next == JobManagerEvent.snapshotMarker()) {
                        logger.info("Emitted: snapshot marker");
                        snapshotRead.set(true);
                    } else if (next instanceof JobUpdateEvent) {
                        Job<?> job = ((JobUpdateEvent) next).getCurrent();
                        logger.info("Emitted job update: jobId={}({}), jobState={}, version={}",
                                job.getId(), next.isArchived() ? "archived" : job.getStatus().getState(), job.getStatus(), job.getVersion()
                        );
                        Optional<EventPropagationTrace> trace = metrics.recordJob(((JobUpdateEvent) next).getCurrent(), !snapshotRead.get());
                        if (printLatency) {
                            trace.ifPresent(t -> {
                                logger.info("Event propagation data: stages={}", t);
                            });
                        }
                    } else if (next instanceof TaskUpdateEvent) {
                        Task task = ((TaskUpdateEvent) next).getCurrent();
                        logger.info("Emitted task update: jobId={}({}), taskId={}, taskState={}, version={}",
                                task.getJobId(), next.isArchived() ? "archived" : task.getStatus().getState(), task.getId(), task.getStatus(), task.getVersion()
                        );
                        Optional<EventPropagationTrace> trace = metrics.recordTask(((TaskUpdateEvent) next).getCurrent(), !snapshotRead.get());
                        if (printLatency) {
                            trace.ifPresent(t -> logger.info("Event propagation data: {}", t));
                        }
                    } else if (next instanceof JobKeepAliveEvent) {
                        logger.info("Keep alive response: " + next);
                    } else {
                        logger.info("Unrecognized event type: {}", next);
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
