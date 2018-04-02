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

import java.util.concurrent.CountDownLatch;

import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import com.netflix.titus.testkit.cli.command.ErrorReports;
import com.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

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
        RxGrpcJobManagementService service = new RxGrpcJobManagementService(context.createChannel());
        Observable<JobChangeNotification> events;

        if (context.getCLI().hasOption('i')) {
            String jobId = context.getCLI().getOptionValue('i');
            events = service.observeJob(JobId.newBuilder().setId(jobId).build());
        } else {
            events = service.observeJobs();
        }

        CountDownLatch latch = new CountDownLatch(1);
        events.subscribe(
                next -> logger.info("Emitted: {}", next),
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
