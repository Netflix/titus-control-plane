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

package io.netflix.titus.testkit.cli.command.job;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import io.netflix.titus.testkit.cli.CliCommand;
import io.netflix.titus.testkit.cli.CommandContext;
import io.netflix.titus.testkit.cli.command.ErrorReports;
import io.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import io.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class JobSubmitCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(JobSubmitCommand.class);

    @Override
    public String getDescription() {
        return "submit a new job";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("f").longOpt("file").argName("file name").hasArg().required()
                .desc("File with job data").build());
        options.addOption(Option.builder("d").longOpt("display")
                .desc("Display file content before sending it").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        JobDescriptor jobDescriptor = loadTemplate(context);

        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcJobManagementService(context.createChannel())
                .createJob(jobDescriptor)
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Job submit succeeded: " + PrettyPrinters.print(result)),
                        e -> ErrorReports.handleReplyError("Job submit error", e)
                );
        latch.await();
    }

    private JobDescriptor loadTemplate(CommandContext context) throws IOException {
        File templateFile = new File(context.getCLI().getOptionValue('f'));

        System.out.println("Submitting job from file " + templateFile);

        JobDescriptor.Builder builder = JobDescriptor.newBuilder();
        try (FileReader fr = new FileReader(templateFile)) {
            JsonFormat.parser().merge(fr, builder);
        }
        JobDescriptor applicajobDescriptorionInfo = builder.build();

        if (context.getCLI().hasOption('d')) {
            System.out.println(JsonFormat.printer().print(applicajobDescriptorionInfo));
        }

        return applicajobDescriptorionInfo;
    }
}
