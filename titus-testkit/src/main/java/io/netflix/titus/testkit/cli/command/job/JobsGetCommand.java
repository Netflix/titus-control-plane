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

import java.util.concurrent.CountDownLatch;

import com.netflix.titus.grpc.protogen.JobQuery;
import com.netflix.titus.grpc.protogen.Page;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.testkit.cli.CliCommand;
import io.netflix.titus.testkit.cli.CommandContext;
import io.netflix.titus.testkit.cli.command.ErrorReports;
import io.netflix.titus.testkit.rx.RxGrpcJobManagementService;
import io.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobsGetCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(JobsGetCommand.class);

    @Override
    public String getDescription() {
        return "get jobs meeting search criteria";
    }

    @Override
    public boolean isRemote() {
        return true;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("a").longOpt("archived").hasArg(false)
                .desc("If set, return also archived jobs").build());
        options.addOption(Option.builder("t").longOpt("type")
                .desc("Search by job type").build());
        options.addOption(Option.builder("s").longOpt("page_size").hasArg().type(Number.class)
                .desc("Maximum number of items to return").build());
        options.addOption(Option.builder("n").longOpt("page_number").hasArg().type(Number.class)
                .desc("Number of page to return starting from 0").build());
        options.addOption(Option.builder("f").longOpt("fields").hasArg()
                .desc("Comma separated list of fields to return").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();

        JobQuery.Builder queryBuilder = JobQuery.newBuilder();

        if (cli.hasOption('a')) {
            queryBuilder.putFilteringCriteria("archived", "true");
        }

        Page.Builder pageBuilder = Page.newBuilder();
        if (cli.hasOption('s')) {
            pageBuilder.setPageSize(((Number) cli.getParsedOptionValue("s")).intValue());
        } else {
            pageBuilder.setPageSize(100);
        }
        if (cli.hasOption('n')) {
            pageBuilder.setPageNumber(((Number) cli.getParsedOptionValue("n")).intValue());
        }
        queryBuilder.setPage(pageBuilder);

        if (cli.hasOption('f')) {
            queryBuilder.addAllFields(StringExt.splitByComma(cli.getOptionValue('f')));
        }

        CountDownLatch latch = new CountDownLatch(1);
        new RxGrpcJobManagementService(context.createChannel())
                .findJobs(queryBuilder.build())
                .doOnUnsubscribe(latch::countDown)
                .subscribe(
                        result -> logger.info("Found jobs: " + PrettyPrinters.print(result)),
                        e -> ErrorReports.handleReplyError("Command execution error", e)
                );
        latch.await();
    }
}
